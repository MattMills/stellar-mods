#!/bin/bash
# Exercise run_periodic_ingest.sh (zip first, then batch ingest) against stub
# scripts in a temp tree. usage: test_run_periodic_ingest.sh LOOP_SCRIPT
P=$(readlink -f "$1")
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
Q=steam_workshop_data/281990
S1=2026-01-01T01:05:01.000001 S2=2026-01-01T02:05:01.000002 S3=2026-01-01T03:05:01.000003
PZ=pending_zip/$Q A=archive_$Q
pass=0 fail=0
check() { if eval "$2"; then echo "  ok   $1"; pass=$((pass+1)); else echo "  FAIL $1"; fail=$((fail+1)); fi; }
n() { ls "$1" 2>/dev/null | wc -l; }
old() { touch -d '2 hours ago' "$@"; }

setup() {
	rm -rf "$T"/* "$T"/.[!.]* 2>/dev/null; cd "$T" || exit 1
	mkdir -p $Q bin
	for s in $S1 $S2 $S3; do mkdir $Q/$s; echo '{}' > $Q/$s/0.json; echo '{}' > $Q/$s/1.json; old $Q/$s; done
	for f in steam_workshop.py postgres_preview_download.py postgres_mod_download.py postgres_mod_file_checksum.py; do
		printf 'open("calls.log", "a").write("%s\\n")\n' $f > $f
	done
	# The batch stub records the snapshots it gets (and whether they are zips)
	# and fails if any matches FAIL_ON.
	cat > postgres_batch_ingest.py <<-'EOF'
	import os, sys
	snaps = [os.path.basename(p)[:-4] if p.endswith('.zip') else '!' + os.path.basename(p) for p in sys.argv[2:]]
	open('calls.log', 'a').write('batch %s\n' % ' '.join(snaps))
	sys.exit(1 if os.environ.get('FAIL_ON') and any(os.environ['FAIL_ON'] in s for s in snaps) else 0)
	EOF
	printf '#!/bin/sh\necho "logger: $*" >> logger.log\n' > bin/logger; chmod +x bin/logger
	sed "s#cd /zpool0/share/stellar-mods/#cd $T#" "$P" > run.sh
}
run() { PATH="$T/bin:$PATH" INGEST_BATCH=2 INGEST_ZIPPERS=2 bash run.sh > out.log 2>&1; echo $?; }
calls() { local c; c=$(grep -c "^$1" calls.log 2>/dev/null); echo "${c:-0}"; }
batches() { grep "^batch" calls.log 2>/dev/null | tr '\n' '|'; }

echo "1. three snapshot folders: zipped, ingested from the zips in order, archived"
setup; rc=$(run)
check "exit 0" '[ "$rc" = 0 ]'
check "batches of zips, oldest first" '[ "$(batches)" = "batch $S1 $S2|batch $S3|" ]'
check "folders gone, pending_zip/ empty" '[ "$(n $Q)" = 0 ] && [ "$(n $PZ)" = 0 ]'
check "3 archives" '[ "$(n $A)" = 3 ]'
check "same paths inside the zip as before" '[ "$(unzip -Z1 $A/$S2.zip | sort | tr "\n" " ")" = "$Q/$S2/0.json $Q/$S2/1.json " ]'
check "fetch ran, downloads ran" '[ "$(calls steam_workshop.py)" = 1 ] && [ "$(calls postgres_mod_download)" = 1 ]'

echo "2. a folder a fetch is still writing, and an empty one from a failed fetch"
setup; mkdir $Q/2026-01-01T09:05:01.000009; mkdir $Q/2025-12-31T00:05:01.000000; old $Q/2025-12-31T00:05:01.000000
rc=$(run)
check "exit 0" '[ "$rc" = 0 ]'
check "fresh folder neither zipped nor ingested" '[ -d $Q/2026-01-01T09:05:01.000009 ] && ! grep -q 09:05 calls.log'
check "empty folder removed, no zip, no ingest" '[ ! -e $Q/2025-12-31T00:05:01.000000 ] && [ ! -e $A/2025-12-31T00:05:01.000000.zip ] && ! grep -q 2025-12-31 calls.log'
check "the rest archived" '[ "$(n $A)" = 3 ]'

echo "3. order: a zip newer than a folder not zipped yet must wait"
setup; mkdir -p $PZ; (zip -q -r -m -9 -o -D $PZ/$S2.zip $Q/$S2 && rmdir $Q/$S2)
touch $Q/$S1  # S1 too young to zip this pass
rc=$(run)
check "exit 0" '[ "$rc" = 0 ]'
check "nothing ingested past S1" '[ "$(calls batch)" = 0 ] && [ -e $PZ/$S2.zip ] && [ -d $Q/$S1 ]'
old $Q/$S1; : > calls.log; rc=$(run)
check "once S1 is zipped: S1, S2, S3 in order" '[ "$(batches)" = "batch $S1 $S2|batch $S3|" ] && [ "$(n $A)" = 3 ]'

echo "4. first batch fails"
setup; rc=$(FAIL_ON=T02 run)
check "exit 1" '[ "$rc" = 1 ]'
check "stopped after the failed batch" '[ "$(batches)" = "batch $S1 $S2|" ]'
check "nothing archived; failed zips kept in pending_zip/" '[ "$(n $A)" = 0 ] && [ -e $PZ/$S1.zip ] && [ -e $PZ/$S2.zip ]'
check "halt file names the batch" '[ "$(cat ingest_halted)" = "281990/$S1 (batch of 2)" ]'
check "logged to syslog; downloads skipped" 'grep -q halted logger.log && [ "$(calls postgres_mod_download)" = 0 ]'

echo "5. rerun while halted, then resume with SKIP_FETCH=1"
: > calls.log; rc=$(run)
check "halted: exit 1, no batch" '[ "$rc" = 1 ] && [ "$(calls batch)" = 0 ] && grep -q "remove .*ingest_halted to resume" out.log'
rm ingest_halted; : > calls.log; rc=$(SKIP_FETCH=1 run)
check "resumed: exit 0, no fetch, all in order" '[ "$rc" = 0 ] && [ "$(calls steam_workshop.py)" = 0 ] && [ "$(batches)" = "batch $S1 $S2|batch $S3|" ]'
check "all 3 archived" '[ "$(n $A)" = 3 ] && [ "$(n $PZ)" = 0 ]'

echo "$pass passed, $fail failed"
[ $fail = 0 ]
