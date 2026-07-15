import json, os
SRC=['/mydata/uber/bignode_state/bagsize_full_day1','/mydata/uber/bignode_state/bagsize_full_day2']
OUT='/mydata/uber/bignode_state/bagsize_pooled_plotdata'; os.makedirs(OUT,exist_ok=True)
# our file prefix -> tool key (label/color/marker comes from BRIDGE_META by key)
MAP=[('pcrb','pb'),('cgprb','cgpb'),('sbridge_fw','structural')]
FIELDS=['amortized_by_total','amortized_by_checkpoint','max_checkpoint_payload',
        'avg_baggage_call','max_baggage_call','num_baggage_calls','num_checkpoint_spans','num_spans']
for srcpref,key in MAP:
    for c in range(2,9):
        merged={f:[] for f in FIELDS}; kept=0; total=0; cpd=c
        ok=False
        for d in SRC:
            p=f"{d}/{srcpref}_cpd{c}.json"
            if not os.path.exists(p): continue
            ok=True
            dd=json.load(open(p)); ns=dd['num_spans']; total+=len(ns)
            mask=[i for i in range(len(ns)) if ns[i]>1]
            kept+=len(mask)
            for f in FIELDS:
                arr=dd.get(f)
                if arr is None: continue
                merged[f].extend(arr[i] for i in mask)
        if not ok: continue
        merged['checkpoint_distance']=cpd
        merged['num_traces']=kept
        outp=f"{OUT}/pooled_{key}_cpd{c}.json"
        json.dump(merged,open(outp,'w'))
        print(f"{key} cpd{c}: kept={kept}/{total} -> {outp}",flush=True)
print("DONE")
