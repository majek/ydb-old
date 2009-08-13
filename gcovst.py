#!/usr/bin/env python
import subprocess
import socket
import time
import sys
import glob


gcoverage = [0, 0]

print "    --------------------------------------------"
for filename in sorted(sys.argv[1:]):
    fd = open(filename, 'rb')
    
    lines = []
    badlines = 0
    for line in fd:
        runs, lineno, line = line.split(":",2)
        runs = runs.strip()
        lineno = int(lineno.strip())
        if runs == "-" or lineno==0:
            continue
        if runs[0] == "#":
            badlines +=1
        lines.append( (runs, lineno) )

    rets = []
    first_run, first_lineno, last_lineno = None, None, None
    for runs, lineno in lines:
        if runs != first_run:
            if first_run:
                if first_run[0] == "#":
                    rets.append( (first_lineno, last_lineno or first_lineno) )
            first_run = runs
            first_lineno = lineno
            last_lineno = None
        else:
            last_lineno = lineno
    if first_run[0] == "#":
        rets.append( (first_lineno, last_lineno or first_lineno) )
    fd.close()
    good, all = (len(lines)-badlines), float(len(lines))
    coverage = (good / all)*100.0
    gcoverage[0] += good
    gcoverage[1] += all
    slots = []
    maxdiff = max(map(lambda (a,b):b-a, rets) or [0])
    maxdiff = max(2, maxdiff)
    for a, b in rets:
        s = "%i-%i" % (a,b) if a!=b else '%i' % a
        if b-a == maxdiff: s = "*" + s + "*"
        slots.append(s)
    print "    %-17s %3.0f%%  %s " % (filename.rpartition(".")[0], coverage, ', '.join(slots))

if gcoverage[1]:
    print "    --------------------------------------------"
    print "    %-17s %3.0f%%" % ('TOTAL', (gcoverage[0]/gcoverage[1]) * 100)
