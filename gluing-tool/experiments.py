'''
Created on Mar 6, 2016

@author: gupta
'''
import csv
import sys
import json
import Queue
from collections import defaultdict
import nltk
from itertools import count
from scipy.stats.morestats import ansari
lmt = nltk.stem.WordNetLemmatizer()
from nltk.corpus import wordnet as wn

word2vec_lim = 0.5

def get_descendants(node, my_rev_taxonomy):
    node_queue = Queue.Queue()
    descendants = {}
    node_queue.put(node)
    
    while not node_queue.empty():
        cur_node = node_queue.get()
        
        if cur_node not in my_rev_taxonomy:
            continue
        
        for child in my_rev_taxonomy[cur_node]:
            if child not in descendants:
                node_queue.put(child)
                descendants[child] = 1
                
    return descendants
         
    

def write_number_of_descendants(my_rev_taxonomy, output_file):
    descans = defaultdict(dict)
    fw = open(output_file,"w")
    count = 0
    for parent in my_rev_taxonomy.keys():
        count += 1
        if count % 20000 == 0:
            print "Done: " + str(count)
        desc = get_descendants(parent, my_rev_taxonomy)
        fw.write(parent + "\t" + "\t".join(desc) + "\n")
        for d in desc:
            descans[parent][d] = 1
    fw.close()    
    
    return descans

def read_descendants(filename):
    descendants = defaultdict(dict)
    with open(filename) as fp:
        for line in fp:
            tokens = line.strip().split('\t')
            for de in tokens[1:]:
                descendants[tokens[0]][de] = 1
    
    return descendants


def load_csv_file(filename):
    print filename
    tokens = []
    with open(filename) as fp:
       # csvr = csv.reader(fp)
        for line in fp:
            tokens.append(line.strip().split("\t"))             
    return tokens

def read_classes(fn):
    ans = {}
    mis = 0
    fin = 0
    for tok in load_csv_file(fn):
        if len(tok) < 2:
            mis += 1
            continue
        fin += 1
        ans[tok[0]] = tok[1]
    
    print "Miss: " + str(mis) + "  Find: " + str(fin)
    return ans 


def read_number_of_instances(fn):
    ans = {}
    for tok in load_csv_file(fn):
        ans[tok[1]] = int(tok[2])
    
    return ans



def rev_multidict(dictv):
    ans = defaultdict(dict)
    for k in dictv.keys():
        for k1 in dictv.keys():
            ans[k1][k] = 1
            
    return ans
        

def rev_dict(dictv):
    ans = {}    
    for k,v in dictv.items():
        ans[v] = k 
    return ans

def read_edges_file(fn, errorfile, keysmap):
    ans = defaultdict(dict)
    missing = {}
    with open(fn) as fp:
        count = 0
        totalcount = 0
        already = 0
        for line in fp:
            totalcount += 1
            tokens = line.strip().split("\t")
            mis = False
            if tokens[0] not in keysmap:
                missing[tokens[0]] = 1
                mis = True
            if tokens[1] not in keysmap:
                missing[tokens[1]] = 1
                mis = True
            if mis:
                count += 1
                continue
            if keysmap[tokens[0]] in ans and keysmap[tokens[1]] in ans[keysmap[tokens[0]]]: 
                already += 1
            ans[keysmap[tokens[0]]][keysmap[tokens[1]]] = 1
                
        print "Missing edges:: " + str(count)    + " out of " + str(totalcount)
        print "Repeat edges:: " + str(already)
    with open(errorfile,"w") as fw:
        for k in missing.keys():
            fw.write(k + "\n")
    return ans
        
        

def read_instance_edges_file(fn, errorfile, instance_keysmap, class_keysmap):
    ans = defaultdict(dict)
    missing = {}
    with open(fn) as fp:
        count = 0
        totalcount = 0
        already = 0
        for line in fp:
            totalcount += 1
            tokens = line.strip().split("\t")
            if len(tokens) < 2:
                continue
            mis = False
            if tokens[0] not in instance_keysmap:
                missing[tokens[0]] = 1
                mis = True
            if tokens[1] not in class_keysmap:
                missing[tokens[1]] = 1
                mis = True
            if mis:
                count += 1
                continue
            if instance_keysmap[tokens[0]] in ans and class_keysmap[tokens[1]] in ans[class_keysmap[tokens[1]]]: 
                already += 1
            ans[class_keysmap[tokens[1]]][instance_keysmap[tokens[0]]] = 1
                
        print "Missing edges:: " + str(count)    + " out of " + str(totalcount)
        print "Repeat edges:: " + str(already)
    with open(errorfile,"w") as fw:
        for k in missing.keys():
            fw.write(k + "\n")
    return ans
        
        
        
        
def rev_taxonomy(tax):
    rev_tax = defaultdict(dict)
    for k in tax.keys():
        for k2 in tax[k].keys():
            rev_tax[k2][k] = 1
    return rev_tax
            

def get_roots(tax, all_nodes):
    rev_tax = rev_taxonomy(tax)
    ans = {}            
    for k in all_nodes.keys():
        if (k not in tax or len(tax[k]) == 0)  and (k in rev_tax and len(rev_tax[k]) > 0):
            ans[k] = 1
    return ans


def convert_camelcase_to_space(strv):
    ans = ""
    for i,s in enumerate(strv):
        if i != 0 and s.isupper():
            ans += " "
        ans += s
    return ans


def read_schema_org_json(filename):
    with open(filename) as fp:
        js = json.load(fp)
        
    classes = {}
    edges = defaultdict(dict)
    
    qu = Queue.Queue()
    qu.put(js)
    
    while not qu.empty():
        cur = qu.get()
        classes[cur['name']] = 1
        
        if 'children' in cur:
            for child in cur['children']:
                edges[child['name']][cur['name']] = 1
                classes[child['name']] = 1
                qu.put(child)
                
    return classes, edges
            	
import time
def run_pos_tag(dictv, server,n1,n2,i,ans,nlp1,f):
    a = time.time()   	
    count = 0
    print "In thread i " + str(i) + " "  + str(n1) + "  "  + str(n2) 
    for i,k in enumerate(dictv.keys()[n1:n2]):
	count += 1
	if i %10 == 0:
            print "Done:: " + str(i)
	try:
   #         print "I am here for--" + str(k)
	    l = nlp1.parse(f(k))
            #print l
	    ans[k] = l
        except Exception as e:
	    print "Error in " + k + " -- " + str(e) 
            pass
    b = time.time()
    print "Time taken for thread: " + str(i) + " = " + str(b - a)
  

import multiprocessing
import corenlp

def run_multi(dictv, server,n1,n2,k):
    a = time.time()
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs = []
    nlps = []
    for i in range(k):
	nlps.append(corenlp.StanfordCoreNLP())

    for i in range(k):
        p = multiprocessing.Process(target=run_pos_tag,args =(dictv,server,n1 + int(i*(n2-n1)/k), min(n2,n1 + int((i+1)*(n2-n1)/k)) + 1,i,return_dict,nlps[i]))
        jobs.append(p)
        p.daemon = True
	p.start()

    for proc in jobs:
	proc.join()

    b = time.time()
    print "Total time taken: " + str(b - a)
    #print return_dict.values()
    return return_dict
 


def get_heads(dictv):
    ans = {}
    for k in dictv.keys():
	js = json.loads(dictv[k])
        try:
	    for d in js['sentences'][0]['dependencies']:
		if d[0] == "root":
		    ans[k] = d[-1]
	except Exception as e:
	    print str(e)
    return ans


def load_vectors(vec_file):
    vec = defaultdict(list)
    with open(vec_file) as fp:
	count = 0
	for line in fp:
	    count += 1
	    if count < 3:
		continue
	    toks = line.strip().split()
            vec[toks[0]] = [float(x) for x in toks[1:]]
    return vec 		

import math
def cosine_sim(d1,d2):
    m = 0
    for i in range(0,len(d1)):
        m += d1[i] * d2[i]

    s1 = sum([v1*v1 for v1 in d1])
    s2 = sum([v2*v2 for v2 in d2])

    if s1 <= 0 or s2 <= 0 or m <= 0:
        return 0

    return m*1.0 / (math.sqrt(s1)*math.sqrt(s2))
    	

    


sim_vec = defaultdict(dict)

def compute_sim(vecs, w1, w2, d):
    if w1 in d and w2 in d[w1]:
	return d[w1][w2]
    sim = 0
    if w1 in vecs and w2 in vecs:
	   sim = cosine_sim(vecs[w1], vecs[w2])
    d[w1][w2] = sim
    d[w2][w1] = sim
    return sim 


def match_heads(vecs,wiki_heads, schema_heads):
    rev_head_schema = defaultdict(list)
    for k,v in schema_heads.items():
	rev_head_schema[v].append(k)

    ans = defaultdict(list)
    for k in wiki_heads.keys():
	head = wiki_heads[k]
        for h2 in rev_head_schema.keys():
	    sim = compute_sim(vecs,k, h2, sim_vec)
            if sim > 0.4:
		ans[k].append(( rev_head_schema[h2],sim))

    return ans	  

def get_lemma_string(postag):
    try:
        js = json.loads(postag)
        l = []
        for k in js["sentences"][0]["words"]:
            l.append(k[1]['Lemma'].lower())
            
        return " ".join(l)
    except Exception as e:
        return None    
        

def get_lemma_string_wn(strv):
    toks = [lemmatize(w).decode('utf-8')  for w in strv.split()]
    return " ".join(toks)


def find_exact_matches(schema_postag, wiki_postag):
    sc_title = {}
    for k in schema_postag.keys():
        l = convert_camelcase_to_space(k.lower())
        sc_title[l] = k
        
    ans = defaultdict(dict)
    for c in wiki_postag.keys():
        if c.lower() in sc_title:
            ans[c][sc_title[c.lower()]] = 1
    return ans



def find_exact_matches_manual(schema_classes, wiki_postag):
    sc_title = {}
    for k in schema_classes.keys():
        for k1 in schema_classes[k].keys():
            sc_title[k1.lower()] = k
            
            
    ans = defaultdict(dict)
    for c in wiki_postag.keys():
        if c.lower() in sc_title:
            ans[c][sc_title[c.lower()]] = 1
    return ans

        
def find_lemma_matches_manual(schema_classes, wiki_postag):
    sc_title = {}
    for k in schema_classes.keys():
        for k1 in schema_classes[k].keys():
            sc_title[k1.lower()] = k
    
    ans = defaultdict(dict)
    for k in wiki_postag.keys():
        lt = get_lemma_string_wn(k)
        if not lt:
            continue
        if lt.lower() in sc_title:
            ans[k][sc_title[lt.lower()]] = 1
    return ans


def find_lemma_matches(schema_postag, wiki_postag):
    sc_title = {}
    for k in schema_postag.keys():
        lt = get_lemma_string(schema_postag[k])
        if not lt:
            continue
	sc_title[lt.lower()] = k
    
    ans = defaultdict(dict)
    for k in wiki_postag.keys():
	lt = get_lemma_string(wiki_postag[k])
        if not lt:
            continue
        if lt.lower() in sc_title:
	    ans[k][sc_title[lt.lower()]] = 1
    return ans
	

def find_head_match_exact(schema_postag, wiki_heads, vecs, val):
    ans = defaultdict(dict)
    sc_title = {}
    for k in schema_postag.keys():
        l = convert_camelcase_to_space(k)
        if len(l.split()) == 1:
            sc_title[l.lower()] = k
    
    for k in wiki_heads.keys():
        if wiki_heads[k] in sc_title:
            ans[k][sc_title[wiki_heads[k]]] = 1                
    
    return ans
    
def find_head_match_word2vec_manual(schema_classes, wiki_heads, vecs, val):
    ans = defaultdict(list)
    sc_title = {}
    for k in schema_classes.keys():
        for k1 in schema_classes[k].keys():
            sc_title[k1.lower()] = k
    
    for k in wiki_heads.keys():
        for h in sc_title.keys():
            sim = compute_sim(vecs,wiki_heads[k],h,sim_vec)
            if sim > val:
                ans[k].append((sc_title[h], sim))    
        
    top_ans = defaultdict(dict)
    for k in ans.keys():
        sv = sorted(ans[k], key = lambda x : -1 * x[1])[0]
        top_ans[k][sv[0]] = sv[1]
    return top_ans

    
def find_match_single_word2vec_manual(schema_classes, wiki_postag, vecs, val):
    ans = defaultdict(list)
    sc_title = {}
    for k in schema_classes.keys():
        for k1 in schema_classes[k].keys():
            sc_title[k1.lower()] = k
    
    for k in wiki_postag.keys():
        tok = k.split()
        if len(tok) == 1:
            for h in sc_title.keys():
                sim = compute_sim(vecs,k,h,sim_vec)
                if sim > val:
                    ans[k].append((sc_title[h], sim))    
                    
    top_ans = defaultdict(dict)
    for k in ans.keys():
        sv = sorted(ans[k], key = lambda x : -1 * x[1])[0]
        top_ans[k][sv[0]] = sv[1]
    return top_ans	  


def find_head_match_exact_manual(schema_classes, wiki_heads, vecs, val):
    ans = defaultdict(dict)
    sc_title = {}
    for k in schema_classes.keys():
        for k1 in schema_classes[k].keys():
            sc_title[k1.lower()] = k
    
    for k in wiki_heads.keys():
        if wiki_heads[k] in sc_title:
            ans[k][sc_title[wiki_heads[k]]] = 1                
    
    return ans


    
def match_desc(vecs,wiki_heads, schema_heads, wiki_desc):
    rev_head_schema = defaultdict(list)
    for k,v in schema_heads.items():
	rev_head_schema[v].append(k)

    ans = defaultdict(dict)
    for k in wiki_heads.keys():
	#head = wiki_heads[k]
        temp_desc = [x for x in wiki_desc[k].keys() if x in wiki_heads][0:50]
        sumv = defaultdict(float)
	countv = defaultdict(int)
        for desc in temp_desc:
            #if desc not in wiki_heads:
            #		continue
            #for w in desc.split():
            	for sc in schema_heads.keys():
			if len(convert_camelcase_to_space(sc).split()) == 1:
	    			s = compute_sim(vecs,wiki_heads[desc], sc.lower(), sim_vec)
         			if s > 0.4:
		    			sumv[sc] += s   #i.append(( rev_head_schema[h2],sim))
                    			countv[sc] += 1
        final_score = {}
	for h2 in countv.keys():
            #for v in rev_head_schema[h2]:
		if sumv[h2] / countv[h2] > 0.7:		
	    		final_score[h2] = sumv[h2] / countv[h2]
	sortedv = sorted(final_score.items(), key = lambda x: -1 * x[1])
 	if len(sortedv) > 0:
		ans[k][sortedv[0][0]] = 1
	    
    return ans	      
    

def read_2col(fn):
    ans = {}
    with open(fn) as fp:
        for line in fp:
            tokens = line.strip().split("\t")
            ans[tokens[0]] = tokens[1]
    
    return ans
    
def compute_coverage(nodes, wiki_desc, wiki_classes_names_to_id):
    cov = {}
    for k in nodes.keys():
        if k in wiki_desc:
            for d in wiki_desc[k]:
                cov[d] = 1
    
    print "Total: " + str(len(wiki_classes_names_to_id)) + "\tCovered: "+  str(len(cov)) + "\tPercent: " + str(len(cov)*100.0/len(wiki_classes_names_to_id))




def get_manual_head(title):
    tok = title.split()
    if len(tok) == 0:
        return None
    if "of" in tok and tok.index("of") > 0:
        return tok[tok.index("of") - 1]
    else:
        return tok[-1]
    
def copy_dict(d1, d2):
    for k in d1.keys():
        for k2 in d1[k]:
            d2[k][k2] = 1
            
            
import itertools

def compute_precision(comb, test):
    output = defaultdict(dict)
    for d in comb:
        for k in d.keys():
            if k in output:
                continue
            for k2 in d[k]:
                output[k][k2] = 1
                
    total = 0
    correct = 0
    for k in test.keys():
        for k2 in test[k]:
            if k in output and k2 in output[k]:
                total += 1
                if test[k][k2] == 1:
                    correct += 1
                    
    prec = 0
    if total > 0:
        prec = correct*100.0/total
    return (prec, total, correct)
    

def test_combinations(lv, test):
    count = 0
    dv = {}
    for i in range(0,len(lv)):
        dv[i] = lv[i]
        
    precision_dict = {}
    for i in range(1,len(lv) + 1):
        l = list(itertools.combinations(dv.keys(),i))
        for m in l:
            for comb in list(itertools.permutations(m)):
                print comb
                count += 1
                precision_dict[" ".join([str(x) for x in comb])] = compute_precision([dv[x] for x in comb], test)
                #break
        #break
        
        
    print count
    #print precision_dict        
    sortedv = sorted(precision_dict.items(), key = lambda x : -1 * x[1][0])
    for i in range(0,5):
        print str(sortedv[i][0]) + "\t" + str(sortedv[i][1])
        
    return precision_dict
    
def read_test_data(fn):
    ans = defaultdict(dict)
    with open(fn) as fp:
        for line in fp:
            tokens = line.split("\t") 
            ans[tokens[0]][tokens[1]] = int(tokens[2])
            
    return ans    
    
    
def print_hypernyms(filename, rels):
    import io
    with io.open(filename, 'w', encoding='utf8') as fw:
        for k in common_all.keys():
            for k2 in common_all[k].keys():
                if k2 in schema_edges:
                    for se in schema_edges[k2].keys():
                        fw.write(k + "\t" + se + "\n")
                else:                 
                    fw.write(k + "\t" + k2 + "\n")
    
                    
                    
                    
def calculate_recursive_num_instances(wiki_desc, instances, wiki_nodes):
    ans = defaultdict(int)
    for k in wiki_nodes.keys():
        val = 0
        if k in instances:
            val += instances[k]
        if k in wiki_desc:
            for v in wiki_desc[k]:
                val += instances[v]
                
        ans[k] = val
    return ans
        
import Queue

def lemmatize_single(w):
    try:
        a = lmt.lemmatize(w).lower()
        return a
    except Exception as e:
        return w

def lemmatize(w):
    x = [lemmatize_single(t) for t in w.split()]
    return " ".join(x)

def get_word(ss):
    tok = ss.name().split(".")
    return  " ".join(" ".join(tok[0:-2]).split("_"))

def get_hypernyms(w):
    w = "_".join(w.split(" "))
    ans = {}
    q = Queue.Queue()
    
    for s in wn.synsets(w):
        q.put(s)
        
    count = 1    
    while not q.empty():
        ss = q.get()
        for h in ss.hypernyms():
            ans[get_word(h)] = count
            count += 1
            q.put(h)
            
    sv = sorted(ans.items(), key = lambda x:  x[1])
    return [x[0] for x in sv]
        



def wn_map_exact(wiki_classes, schema_classes):
    sc = {}
    for k in schema_classes.keys():
        sc[lemmatize(convert_camelcase_to_space(k).lower())] = 1
    ans = defaultdict(dict)
    
    for c in wiki_classes.keys():
        try:
            hyp = get_hypernyms(c)
            for h in hyp:
                if lemmatize(h) in sc:
                    ans[c][h] = 1
                    break
                
        except Exception as e:
            pass            
    return ans
    
    
    
    
    


#def wn_map_lemma():    


def read_schema_classes(schema_manual):
    ans = defaultdict(dict)
    with open(schema_manual) as fp:
        for line in fp.readlines():
            tok = line.strip().split('\t')
            title = convert_camelcase_to_space(tok[0])
            if len(title.split()) == 1:
                ans[tok[0]][tok[0].lower()] = 1
            for k in tok[1:]:
                ans[tok[0]][k] = 1
    return ans
                
                
                

        
                
def match_instance_manual(schema_classes, wiki_manual_heads, vecs,val, wiki_instance_edges):
    ans = defaultdict(dict)
    sc_title = {}
    for k in schema_classes.keys():
        for k1 in schema_classes[k].keys():
            sc_title[k1.lower()] = k
    
    
    print len(wiki_manual_heads.keys())
    for i,k in enumerate(wiki_manual_heads.keys()):
        if i % 1000 == 0:
            print "Done " + str(i)
        temp_desc = [x.lower() for x in wiki_instance_edges[k].keys() if len(x.split()) == 1 ][0:100]
        sumv = defaultdict(float)
        countv = defaultdict(int)
        for desc in temp_desc:
            for sc1 in schema_classes.keys():
                for sc2 in schema_classes[k].keys():
                    s = compute_sim(vecs, sc2.lower(), desc, sim_vec)
                    if s > word2vec_lim:
                       # print "matched " + str(sc2) + "--" + str(desc)

                        sumv[sc1] += s   #i.append(( rev_head_schema[h2],sim))
                        countv[sc1] += 1
        final_score = {}
     #   if (len(countv) > 0):
    #        print "haha"
        for h2 in countv.keys():
                final_score[h2] = sumv[h2]*1.0 / countv[h2]
        sortedv = sorted(final_score.items(), key = lambda x: -1 * x[1])
        if len(sortedv) > 0:
            ans[k][sortedv[0][0]] = 1
        
    return ans

    

if __name__ == '__main__':
    print sys.argv
    #import jsonrpc
    #server = jsonrpc.ServerProxy(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=("127.0.0.1", 3456)))
    wiki_classes_id_to_names = read_classes(sys.argv[1])
    wiki_classes_num_instances = read_number_of_instances(sys.argv[2])
    wiki_edges = read_edges_file(sys.argv[3], sys.argv[4], wiki_classes_id_to_names)
    wiki_classes_names_to_id = rev_dict(wiki_classes_id_to_names)
    wiki_roots = get_roots(wiki_edges, wiki_classes_names_to_id)
    schema_classes, schema_edges = read_schema_org_json(sys.argv[5])
    wiki_desc = write_number_of_descendants(rev_taxonomy(wiki_edges), "wiki_desc")
    wiki_postag = read_2col(sys.argv[6])
    schema_postag = read_2col(sys.argv[7])
    vecs = load_vectors(sys.argv[8])
    schema_classes_manual = read_schema_classes(sys.argv[9])
    wiki_instances_id_to_names = read_classes(sys.argv[10])
    wiki_instances_names_to_id = rev_dict(wiki_instances_id_to_names)
    print sys.argv[11]
    wiki_instance_edges = read_instance_edges_file(sys.argv[11], sys.argv[12], wiki_instances_id_to_names, wiki_classes_id_to_names)
  #  test = read_test_data(sys.argv[8])
    #print "here1"
    #rev_wiki_instance_edges = rev_multidict(wiki_instance_edges)
    #print "here2"
    
    rev_wiki_subclass_edges = {} # rev_multidict(wiki_edges)
    #print "here3"
    
    
   # wiki_heads = get_heads(wiki_classes_names_to_id)
    #print "here4"
    
    schema_heads = get_heads(schema_postag)
    #print "here5"
    
    wiki_manual_heads = {}
    for k in wiki_classes_names_to_id.keys():                          
        h = get_manual_head(k)   
        if h:
            wiki_manual_heads[k] = h
    
    common_exact = find_exact_matches_manual(schema_classes_manual, wiki_classes_names_to_id)
    common_lemma = find_lemma_matches_manual(schema_classes_manual, wiki_classes_names_to_id)
    #common_wn_map_exact = wn_map_exact(wiki_classes_names_to_id, schema_classes)
    common_single_word2vec = find_match_single_word2vec_manual(schema_classes_manual, wiki_classes_names_to_id, vecs,word2vec_lim)
    common_head_match_exact = find_head_match_exact_manual(schema_classes_manual, wiki_manual_heads, vecs, word2vec_lim)    	
    common_head_match_word2vec = find_head_match_word2vec_manual(schema_classes_manual, wiki_manual_heads, vecs, word2vec_lim)
    #common_desc = match_desc(vecs,wiki_manual_heads, schema_heads, wiki_desc)
    
    
    revt = rev_taxonomy(wiki_edges)
    common_instance = match_instance_manual(schema_classes_manual, wiki_manual_heads, vecs,word2vec_lim, wiki_instance_edges)
    common_subclass = match_instance_manual(schema_classes_manual, wiki_manual_heads, vecs,word2vec_lim, revt)
    
  #  pdict = test_combinations([common_exact, common_lemma, common_single_word2vec, common_head_match_exact, common_head_match_word2vec, common_desc])
    
    
    common_all = defaultdict(dict)
    copy_dict(common_exact, common_all)
    copy_dict(common_lemma, common_all)
    copy_dict(common_single_word2vec, common_all)
    copy_dict(common_head_match_exact, common_all)
    copy_dict(common_head_match_word2vec, common_all)
    copy_dict(common_instance, common_all)
    copy_dict(common_subclass, common_all)
    compute_coverage(common_all, wiki_desc, wiki_classes_names_to_id)
    
    #copy_dict(common_wn_map_exact, common_all)
    
#   
    #copy_dict(common_head_match_exact, common_all)
#    copy_dict(common_head_match_word2vec, common_all)
    #copy_dict(common_desc, common_all)
    
    f = open('results.txt', 'w')
    for src, value in common_all.iteritems() :
        for dst, val in value.iteritems() : 
            f.write(src + '\t' + dst + '\n')
    
#count = 0
#for i,d in enumerate([common_exact, common_lemma, common_single_word2vec, common_head_match_exact, common_head_match_word2vec, common_desc]):
#    for j in range(0,50):
#        s = random.choice(d.keys())
#        print str(i) + "\t" + s + "\t" + "\t".join(d[s])
    
    
    
