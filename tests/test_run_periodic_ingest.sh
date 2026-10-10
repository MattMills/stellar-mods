#!/bin/bash
# Exercise the batch run_periodic_ingest.sh against stub scripts in a temp tree.
# usage: test_batch_shell.sh LOOP_SCRIPT ZIPPER_SCRIPT
P=$(readlink -f "$1") Z=$(readlink -f "$2")
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
Q=steam_workshop_data/281990
S1=2026-01-01T01:05:01.000001 S2=2026-01-01T02:05:01.000002 S3=2026-01-01T03:05:01.000003
pass=0 fail=0
check() { if eval "$2"; then echo "  ok   $1"; pass=$((pass+1)); else echo "  FAIL $1"; fail=$((fail+1)); fi; }

setup() {
	rm -rf "$T"/* "$T"/.[!.]* 2>/dev/null; cd "$T" || exit 1
	mkdir -p $Q bin
	for s in $S1 $S2 $S3; do mkdir $Q/$s; echo '{}' > $Q/$s/0.json; echo '{}' > $Q/$s/1.json; done
	touch -d '2026-01-01 01:10' $Q/$S1; touch -d '2026-01-01 02:10' $Q/$S2; touch -d '2026-01-01 03:10' $Q/$S3
	for f in steam_workshop.py postgres_preview_download.py postgres_mod_download.py postgres_mod_file_checksum.py; do
		printf 'open("calls.log", "a").write("%s\\n")\n' $f > $f
	done
	# The batch stub fails if any snapshot matches FAIL_ON; while ingesting S3 it
	# also plays a concurrent fetch: a fresh empty folder (must survive) and an old
	# empty one from a failed fetch (must be swept).
	cat > postgres_batch_ingest.py <<-'EOF'
	import os, sys, time
	snaps = [os.path.basename(d.rstrip('/')) for d in sys.argv[2:]]
	with open('calls.log', 'a') as f:
	    f.write('batch %s\n' % ' '.join(snaps))
	if any(s.startswith('2026-01-01T03') for s in snaps):
	    os.makedirs('steam_workshop_data/281990/2026-01-01T09:05:01.000009', exist_ok=True)
	    old = 'steam_workshop_data/281990/2025-12-31T00:05:01.000000'
	    os.makedirs(old, exist_ok=True)
	    os.utime(old, (time.time() - 7200, time.time() - 7200))
	sys.exit(1 if os.environ.get('FAIL_ON') and any(os.environ['FAIL_ON'] in s for s in snaps) else 0)
	EOF
	printf '#!/bin/sh\necho "logger: $*" >> logger.log\n' > bin/logger; chmod +x bin/logger
	sed "s#cd /zpool0/share/stellar-mods/#cd $T#" "$P" > run.sh
	sed "s#cd /zpool0/share/stellar-mods/#cd $T#" "$Z" > zip.sh
}
run() { PATH="$T/bin:$PATH" INGEST_BATCH=2 bash run.sh > out.log 2>&1; echo $?; }
zipper() { ONCE=1 bash zip.sh > zip.log 2>&1; echo $?; }
calls() { local c; c=$(grep -c "^$1" calls.log 2>/dev/null); echo "${c:-0}"; }
batches() { grep "^batch" calls.log 2>/dev/null | tr '\n' '|'; }
C=complete/$Q U=under_zip/$Q A=archive_$Q
n() { ls "$1" 2>/dev/null | wc -l; }

echo "1. all snapshots succeed (3 snapshots, batches of 2)"
setup; rc=$(run)
check "exit 0" '[ "$rc" = 0 ]'
check "two batches, in order" '[ "$(batches)" = "batch $S1 $S2|batch $S3|" ]'
check "all three handed to complete/, none zipped by the loop" '[ "$(n $C)" = 3 ] && [ "$(n $A)" = 0 ]'
check "waiting folder emptied" '[ ! -e $Q/$S1 ] && [ ! -e $Q/$S2 ] && [ ! -e $Q/$S3 ]'
check "old empty folder swept" '[ ! -e $Q/2025-12-31T00:05:01.000000 ]'
check "fresh empty folder (fetch in flight) kept" '[ -d $Q/2026-01-01T09:05:01.000009 ]'
check "fetch ran" '[ "$(calls steam_workshop.py)" = 1 ]'
check "downloads ran" '[ "$(calls postgres_mod_download)" = 1 ]'
rc=$(zipper)
check "zipper: 3 archives" '[ "$(n $A)" = 3 ]'
check "zipper: same paths inside the zip as before" '[ "$(unzip -Z1 $A/$S2.zip | sort | tr "\n" " ")" = "$Q/$S2/0.json $Q/$S2/1.json " ]'
check "zipper: complete/ and under_zip/ empty" '[ "$(n $C)" = 0 ] && [ "$(n $U)" = 0 ]'

echo "2. first batch fails"
setup; rc=$(FAIL_ON=T02 run)
check "exit 1" '[ "$rc" = 1 ]'
check "stopped after the failed batch" '[ "$(batches)" = "batch $S1 $S2|" ]'
check "nothing handed over" '[ "$(n $C)" = 0 ]'
check "all folders intact" '[ "$(ls $Q/$S1 $Q/$S2 $Q/$S3 | grep -c json)" = 6 ]'
check "halt file names the batch" '[ "$(cat ingest_halted)" = "281990/$S1 (batch of 2)" ]'
check "logged to syslog" 'grep -q "halted" logger.log'
check "downloads skipped" '[ "$(calls postgres_mod_download)" = 0 ]'

echo "3. second batch fails"
setup; rc=$(FAIL_ON=T03 run)
check "exit 1" '[ "$rc" = 1 ]'
check "first batch handed over" '[ -d $C/$S1 ] && [ -d $C/$S2 ] && [ ! -e $Q/$S1 ]'
check "failed batch left in place" '[ ! -e $C/$S3 ] && [ "$(n $Q/$S3)" = 2 ]'
check "halt file names it" '[ "$(cat ingest_halted)" = "281990/$S3 (batch of 1)" ]'

echo "4. rerun while halted"
: > calls.log; rc=$(run)
check "exit 1" '[ "$rc" = 1 ]'
check "no batch attempted" '[ "$(calls batch)" = 0 ]'
check "says how to resume" 'grep -q "remove .*ingest_halted to resume" out.log'

echo "5. halt removed, cause fixed; SKIP_FETCH=1"
# Left over from 3: an old empty folder (a failed fetch, ingested like any
# snapshot) and a fresh one (a fetch still writing: SKIP_FETCH must leave it).
rm ingest_halted; : > calls.log; rc=$(SKIP_FETCH=1 run)
check "exit 0" '[ "$rc" = 0 ]'
check "no fetch" '[ "$(calls steam_workshop.py)" = 0 ]'
check "resumes at S3, skips the folder being fetched" '[ "$(batches)" = "batch $S3 2025-12-31T00:05:01.000000|" ]'
check "folder being fetched untouched" '[ -d $Q/2026-01-01T09:05:01.000009 ]'
rc=$(zipper)
check "zipper: all 3 archived" '[ "$(n $A)" = 3 ]'
check "zipper: empty failed-fetch folder removed, not archived" '[ ! -e $C/2025-12-31T00:05:01.000000 ] && [ ! -e $U/2025-12-31T00:05:01.000000 ] && [ ! -e $A/2025-12-31T00:05:01.000000.zip ]'

echo "6. zipper crash recovery"
setup; mkdir -p $U/$S1 && mv $Q/$S1/* $U/$S1/ && rmdir $Q/$S1
rc=$(zipper)
check "left-over under_zip/ folder zipped on the next pass" '[ -e $A/$S1.zip ] && [ ! -e $U/$S1 ]'

echo "$pass passed, $fail failed"
[ $fail = 0 ]
