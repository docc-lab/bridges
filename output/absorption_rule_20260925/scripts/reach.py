"""Reachable-function line counts from the three paper entry points.

Strips comments/strings (same rules as goloc.py), extracts top-level funcs
(gofmt guarantees `func ` at column 0 and a closing `}` at column 0), then BFS
over "callee identifier appears as a word in caller body". Over-approximates
slightly (a name used as a value counts as a call) and cannot see through
interface dispatch, so treat it as an upper bound on reachable code.
"""
import re, sys, glob, json
from collections import defaultdict

def strip(src):
    out=[]; i=0; n=len(src); state=None
    while i<n:
        c=src[i]
        if state is None:
            if c=='/' and i+1<n and src[i+1]=='/':
                while i<n and src[i]!='\n': i+=1
                continue
            if c=='/' and i+1<n and src[i+1]=='*':
                i+=2
                while i+1<n and not (src[i]=='*' and src[i+1]=='/'):
                    if src[i]=='\n': out.append('\n')
                    i+=1
                i+=2; continue
            if c in '"`\'': state=c; out.append(' '); i+=1; continue
            out.append(c); i+=1; continue
        if state in ('"',"'") and c=='\\': i+=2; continue
        if c==state: state=None
        if c=='\n': out.append('\n')
        i+=1
    return ''.join(out)

FUNC=re.compile(r'^func\s+(?:\([^)]*\)\s*)?([A-Za-z_][A-Za-z0-9_]*)')
funcs={}           # name -> list of (file, loc)
bodies={}          # name -> body text (concatenated if overloaded by receiver)
for path in sorted(glob.glob('recon/*.go')):
    if path.endswith('_test.go'): continue
    lines=strip(open(path).read()).split('\n')
    i=0
    while i<len(lines):
        m=FUNC.match(lines[i])
        if not m: i+=1; continue
        name=m.group(1); start=i; i+=1
        while i<len(lines) and not (lines[i].startswith('}')): i+=1
        body='\n'.join(lines[start:i+1])
        loc=sum(1 for l in lines[start:i+1] if l.strip())
        funcs.setdefault(name,[]).append((path,loc))
        bodies[name]=bodies.get(name,'')+'\n'+body
        i+=1

names=set(funcs)
WORD=re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
calls={n:{w for w in WORD.findall(bodies[n]) if w in names and w!=n} for n in names}

def reach(entry):
    seen={entry}; stack=[entry]
    while stack:
        f=stack.pop()
        for c in calls.get(f,()):
            if c not in seen: seen.add(c); stack.append(c)
    return seen

E={'pb0':'ReconstructPB0','cgp0':'ReconstructCGP0','sb3':'ReconstructSB3WithDEE'}
R={k:reach(v) for k,v in E.items()}
def loc(fs): return sum(l for f in fs for _,l in funcs[f])
print('reachable functions / non-blank non-comment lines')
for k in E: print(f'  {k:5} {len(R[k]):5} funcs  {loc(R[k]):6} lines')
allf=R['pb0']|R['cgp0']|R['sb3']
print(f'  union {len(allf):5} funcs  {loc(allf):6} lines')
print()
excl={k:R[k]-set().union(*(R[j] for j in E if j!=k)) for k in E}
shared_all=R['pb0']&R['cgp0']&R['sb3']
print('partition')
for k in E: print(f'  exclusive to {k:5} {len(excl[k]):4} funcs  {loc(excl[k]):6} lines')
print(f'  shared by all 3     {len(shared_all):4} funcs  {loc(shared_all):6} lines')
rest=allf-shared_all-set().union(*excl.values())
print(f'  shared by exactly 2 {len(rest):4} funcs  {loc(rest):6} lines')
print()
byfile=defaultdict(int)
for f in allf:
    for p,l in funcs[f]: byfile[p]+=l
print('reachable lines by file')
for p,l in sorted(byfile.items(), key=lambda x:-x[1]): print(f'  {l:6} {p}')
json.dump({k:sorted(R[k]) for k in R}, open('/tmp/claude-36637/-users-tomislav/ffcf4f52-9bc3-4953-9cf9-ff8df2039b15/scratchpad/reach.json','w'))
