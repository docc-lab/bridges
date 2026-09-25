"""Non-blank, non-comment Go line counter.

Strips // and /* */ comments while respecting interpreted strings ("..."),
raw strings (`...`, which may span lines and contain no escapes) and rune
literals ('...'), then counts lines with any non-whitespace remaining.
"""
import sys

def count(src):
    out=[]; i=0; n=len(src); line=[]; state=None
    while i<n:
        c=src[i]
        if state is None:
            if c=='/' and i+1<n and src[i+1]=='/':
                while i<n and src[i]!='\n': i+=1
                continue
            if c=='/' and i+1<n and src[i+1]=='*':
                i+=2
                while i+1<n and not (src[i]=='*' and src[i+1]=='/'):
                    if src[i]=='\n': line.append('\n')
                    i+=1
                i+=2
                continue
            if c=='"': state='"'; line.append(c); i+=1; continue
            if c=='`': state='`'; line.append(c); i+=1; continue
            if c=="'": state="'"; line.append(c); i+=1; continue
            line.append(c); i+=1; continue
        # inside a literal
        if state in ('"',"'") and c=='\\':
            line.append(src[i:i+2]); i+=2; continue
        if c==state: state=None
        line.append(c); i+=1
    text=''.join(line)
    return sum(1 for l in text.split('\n') if l.strip())

for p in sys.argv[1:]:
    print(f'{count(open(p).read())}\t{p}')
