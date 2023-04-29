import math, re
import Stemmer
import sys, os, timeit
from collections import defaultdict

indices_folder = './indices'
queries_output = 'queries_op.txt​'
stopwords = ['a','about','above','across','after','afterwards','again','against','all','almost','alone','along','already','also','although','always','am','among','amongst','amount','an','and','another','any','anyhow','anyone','anything','anyway','anywhere','are','around','as','at','back','be','became','because','become','becomes','becoming','been','before','beforehand','behind','being','below','beside','besides','between','beyond','bill','both','bottom','but','by','call','can','cannot','cant','co','computer','con','could','couldnt','cry','de','describe','detail','do','done','down','due','during','each','eg','eight','either','eleven','else','elsewhere','empty','enough','etc','even','ever','every','everyone','everything','everywhere','except','few','fifteen','fify','fill','find','fire','first','five','for','former','formerly','forty','found','four','from','front','full','further','get','give','go','had','has','hasnt','have','he','hence','her','here','hereafter','hereby','herein','hereupon','hers','herself','him','himself','his','how','however','hundred','i','ie','if','in','inc','indeed','interest','into','is','it','its','itself','keep','last','latter','latterly','least','less','ltd','made','many','may','me','meanwhile','might','mill','mine','more','moreover','most','mostly','move','much','must','my','myself','name','namely','neither','never','nevertheless','next','nine','no','nobody','none','noone','nor','not','nothing','now','nowhere','of','off','often','on','once','one','only','onto','or','other','others','otherwise','our','ours','ourselves','out','over','own','part','per','perhaps','please','put','rather','re','same','see','seem','seemed','seeming','seems','serious','several','she','should','show','side','since','sincere','six','sixty','so','some','somehow','someone','something','sometime','sometimes','somewhere','still','such','system','take','ten','than','that','the','their','them','themselves','then','thence','there','thereafter','thereby','therefore','therein','thereupon','these','they','thick','thin','third','this','those','though','three','through','throughout','thru','thus','to','together','too','top','toward','towards','twelve','twenty','two','un','under','until','up','upon','us','very','via','was','we','well','were','what','whatever','when','whence','whenever','where','whereafter','whereas','whereby','wherein','whereupon','wherever','whether','which','while','whither','who','whoever','whole','whom','whose','why','will','with','within','without','would','yet','you','your','yours','yourself','yourselves']

def tokenize(text):  
    text = text.encode("ascii", errors="ignore").decode()
    text = re.sub(r'[^A-Za-z0-9]+', r' ', text) #Replace special char with space
    return text.split()   # Split on white-space

def remove_stopwords(text):
    return [w for w in text if stops.get(w) != True]  # keep term if not in stops dictionary

def stem(text):
    return stemmer.stemWords(text)

def get_pointers(offset, term, high, f, typ='str'):
    low = 0
    while True:
        if low < high :
            seek_pos = int((low + high) / 2)
            f.seek(offset[seek_pos])
            term_pointers = f.readline().strip().split()
            if typ == 'int': 
                if int(term_pointers[0]) == int(term):
                    return seek_pos, term_pointers[1:]
                elif int(term) <=int(term_pointers[0]):
                    high = seek_pos
                else:
                    low = seek_pos + 1
            else:
                if term_pointers[0] == term :
                    return seek_pos, term_pointers[1:]
                elif term <= term_pointers[0]:
                    high = seek_pos
                else:
                    low = seek_pos + 1
        else :
            break
    return -1,[]

def doc_find(term, file_num, field ,field_file):
    fieldOffset, doc_freq = [],[]
    file_path = os.path.join(indices_folder, 'offset_' + field + file_num + '.txt' )

    f = open(file_path, 'r')
    for line in f:
        offset, df = line.strip().split()
        doc_freq.append(int(df))
        fieldOffset.append(int(offset))
    seek_pos, doc_list = get_pointers(fieldOffset, term, len(fieldOffset), field_file)
    return doc_freq[seek_pos], doc_list


def ranked_docs(total_docs, results, doc_freq, support=False):
    fields = ['l', 'b', 'i', 't', 'c']
    if(support==False):
        weights = [0.05, 0.40, 0.10, 0.35, 0.10] # for plain query - more weightage to body
    else:
        weights = [0.05, 0.30, 0.10, 0.45, 0.10] 
    queryIdf = {}
    docs = defaultdict(float)
    for key in doc_freq:
        num = (float(total_docs) - float(doc_freq[key]) + 0.5) / ( float(doc_freq[key]) + 0.5)
        queryIdf[key] = math.log(num)
        tem = float(total_docs) / float(doc_freq[key])
        doc_freq[key] = math.log(tem)
    for term in results:
        fieldWisePostingList = results[term]
        for field in fieldWisePostingList:
            if len(field) > 0:
                postingList = fieldWisePostingList[field]                
                weight = weights[fields.index(field)]
                i=0
                size_post = len(postingList)
                while i < size_post:
                    tfidf = (1 + math.log(float(postingList[i+1])) ) * doc_freq[term]
                    docs[postingList[i]] += float( tfidf * weight )
                    i+=2
    return docs

def queries_file_search(indices, queries_file):
    out_f = open(queries_output, 'w')
    for line in queries_file:
        K, query_string = line.strip().split(',')
        if len(query_string.strip()) > 0:
            #out_f.write('\n\n=========== ' + query_string)
            print('\n=========== ' + query_string)
            search_string(indices, query_string.strip(), file_out=out_f, K=int(K.strip()))
            out_f.write('\n')            
    out_f.close()
    print ('Queries output saved at', queries_output )

def command_line_search(indices):
    while True:
        query_string = input('\nEnter Search Query :')
        search_string(indices, query_string)

def search_string(indices, query_string, file_out=False, K=10):

    title_offset = indices['title_offset']
    offset       = indices['offset'      ] 
    total_docs   = indices['total_docs'  ] 
    title_file   = indices['title_file'  ] 
    vocab_file   = indices['vocab_file'  ] 

    tem_val    = len(title_offset)

    query_string = query_string.lower()
    start = timeit.default_timer()
    if re.match(r'[t|b|i|c|l]:', query_string):
        tempFields = re.findall(r'([t|b|c|i|l]):', query_string)
        terms = re.findall(r'[t|b|c|i|l]:([^:]*)(?!\S)', query_string)
        fields,tokens = [],[]
        si = len(terms)
        i=0
        while i<si:
            for term in terms[i].split():
                fields.append(tempFields[i])
                tokens.append(term)
            i+=1
        tokens = remove_stopwords(tokens)
        tokens = stem(tokens)
        results, doc_freq = fields_query(tokens, fields, vocab_file, offset)
        results = ranked_docs(total_docs, results, doc_freq, True)
    else:
        tokens = tokenize(query_string)
        tokens = remove_stopwords(tokens)
        tokens = stem(tokens)
        results, doc_freq = plain_query(vocab_file,tokens, offset)
        results = ranked_docs(total_docs, results, doc_freq)

    stdout = '-------\nTop '+ str(K) + ' Matching Documents:'
    print(stdout)
    #if file_out:
    #    file_out.write(stdout)

    if len(results) > 0:
        results = sorted(results, key=results.get, reverse=True)
        results = results[:K]

        for key in results:
            _,title = get_pointers(title_offset, key,tem_val, title_file, 'int')
            stdout = key+ ',' + ' '.join(title)
            print(stdout)
            if file_out:
                file_out.write('\n' + stdout)

    end = timeit.default_timer()
    stdout = '---------Search time = '+ str(end-start) + ' seconds'
    print(stdout)
    if file_out:
        file_out.write('\n' + str(end-start) + ', ' + str((end-start)/K) )

def fields_query(terms, fields, vocab_file, offset):
    doc_list = defaultdict(dict)
    doc_freq = {}
    n_terms = len(terms)
    i=0
    while i < n_terms:
        term = terms[i]
        field = fields[i]
        mid,docs = get_pointers(offset, term, len(offset), vocab_file)
        if len(docs) > 0:
            file_num = docs[0]
            filename = os.path.join(indices_folder, field + str(file_num) + '.txt')
            field_file = open(filename, 'r')
            df,returned_list = doc_find(term, file_num, field, field_file)
            doc_freq[term] = df
            doc_list[term][field] = returned_list
        i+=1
    return doc_list, doc_freq


def plain_query(vocab_file, terms, offset):
    doc_freq,fields = {}, ['l', 'b', 'i', 't', 'c']
    doc_list = defaultdict(dict)
    for term in terms:
        mid,docs = get_pointers(offset, term, len(offset), vocab_file)
        if len(docs) > 0:
            file_num = docs[0]
            doc_freq[term] = docs[1]
            for field in fields:
                filename = os.path.join(indices_folder, field + str(file_num) + '.txt')
                field_file = open(filename, 'r')
                _,returned_list = doc_find(term,file_num, field, field_file)
                doc_list[term][field] = returned_list
    return doc_list, doc_freq

def load_search_engine():
    print('Loading Search Engine.......................\n')
    offset,title_offset = [],[]

    f = open(os.path.join(indices_folder, 'titleOffset.txt' ) , 'r')
    for line in f:
        title_offset.append(int(line.strip()))
    f.close()

    f = open(os.path.join(indices_folder, 'offset.txt' ), 'r')
    for line in f:
        offset.append(int(line.strip()))
    f.close()

    f = open(os.path.join(indices_folder, 'total_docs.txt'), 'r')
    total_docs = int(f.read().strip())
    f.close()

    title_file  = open(os.path.join(indices_folder, 'title.txt'), 'r')
    vocab_file = open(os.path.join(indices_folder, 'vocab.txt'), 'r')

    indices               = {}
    indices['title_offset'] = title_offset
    indices['offset'      ] = offset
    indices['total_docs'  ] = total_docs
    indices['title_file'  ] = title_file
    indices['vocab_file'  ] = vocab_file

    return indices

if __name__ == '__main__':

    stops = {}
    for term in stopwords:
    	stops[term] = True

    stemmer = Stemmer.Stemmer('english')

    indices = load_search_engine()

    try:
        queries_file = open(sys.argv[1])
        queries_file_search(indices, queries_file)
    except:
        print ('A queries.txt of the format ​ K, query_string was expected in the first argument.')
        print ('Falling back to command-line search mode with default Top (K=)10 Documents.')
        command_line_search(indices)
