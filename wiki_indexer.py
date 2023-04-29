from collections import defaultdict
from time import time, gmtime, strftime
import xml.sax
import sys, re, os
# from nltk.stem import PorterStemmer 
import Stemmer
# from gensim.parsing.porter import PorterStemmer
import heapq
import operator
import threading
from tqdm import tqdm

indices_folder = './indices' 
stopwords = ['a','about','above','across','after','afterwards','again','against','all','almost','alone','along','already','also','although','always','am','among','amongst','amount','an','and','another','any','anyhow','anyone','anything','anyway','anywhere','are','around','as','at','back','be','became','because','become','becomes','becoming','been','before','beforehand','behind','being','below','beside','besides','between','beyond','bill','both','bottom','but','by','call','can','cannot','cant','co','computer','con','could','couldnt','cry','de','describe','detail','do','done','down','due','during','each','eg','eight','either','eleven','else','elsewhere','empty','enough','etc','even','ever','every','everyone','everything','everywhere','except','few','fifteen','fify','fill','find','fire','first','five','for','former','formerly','forty','found','four','from','front','full','further','get','give','go','had','has','hasnt','have','he','hence','her','here','hereafter','hereby','herein','hereupon','hers','herself','him','himself','his','how','however','hundred','i','ie','if','in','inc','indeed','interest','into','is','it','its','itself','keep','last','latter','latterly','least','less','ltd','made','many','may','me','meanwhile','might','mill','mine','more','moreover','most','mostly','move','much','must','my','myself','name','namely','neither','never','nevertheless','next','nine','no','nobody','none','noone','nor','not','nothing','now','nowhere','of','off','often','on','once','one','only','onto','or','other','others','otherwise','our','ours','ourselves','out','over','own','part','per','perhaps','please','put','rather','re','same','see','seem','seemed','seeming','seems','serious','several','she','should','show','side','since','sincere','six','sixty','so','some','somehow','someone','something','sometime','sometimes','somewhere','still','such','system','take','ten','than','that','the','their','them','themselves','then','thence','there','thereafter','thereby','therefore','therein','thereupon','these','they','thick','thin','third','this','those','though','three','through','throughout','thru','thus','to','together','too','top','toward','towards','twelve','twenty','two','un','under','until','up','upon','us','very','via','was','we','well','were','what','whatever','when','whence','whenever','where','whereafter','whereas','whereby','wherein','whereupon','wherever','whether','which','while','whither','who','whoever','whole','whom','whose','why','will','with','within','without','would','yet','you','your','yours','yourself','yourselves']

def tokenize(text):
    text = text.encode("ascii", errors="ignore").decode()
    text = re.sub(r'[^A-Za-z0-9]+', r' ', text) #Replace special char with space
    return text.split()

def remove_stopwords(text):
    return [w for w in text if stops.get(w) != True]
    #return [w for w in text if w not in stopwords]

def stem(text):
    # return stemmer.stem_documents(text)     # gensim
    # return [stemmer.stem(w) for w in text]  # nltk
    return stemmer.stemWords(text)            # Pystemmer

def process_text(text, title):
    # info, body, title above references
    # references, Links, categories below references
    text = text.lower() #case folding
    temp = text.split("== references == ") 

    if len(temp) == 1:
        temp = text.split('st2')
    if len(temp) == 1: # If empty then initialize with empty lists
        links      = []
        categories = []
    else: 
        categories = process_categories(temp[1])
        links = process_links(temp[1])

    body  = process_body(temp[0])
    title = process_title(title.lower())
    info  = process_info(temp[0])

    return title, body, info, categories, links

def process_title(text):
    title = tokenize(text)
    title = remove_stopwords(title)
    title = stem(title)
    return title

def process_body(text):
    body = re.sub(r'\{\{.*\}\}', r' ', text)
    body = tokenize(body)
    body = remove_stopwords(body)
    body = stem(body)
    return body

def process_info(text):
    text = text.split('\n')
    fl = -1
    info = []

    for line in text:
        if re.match(r'\{\{infobox', line):
            info.append(re.sub(r'\{\{infobox(.*)', r'\1', line))
            fl = 0
        elif fl == 0:
            if line == "}}":
                fl = -1
                continue
            info.append(line)
    info = tokenize(' '.join(info))
    info = remove_stopwords(info)
    info = stem(info)
    return info

def process_categories(text):
    text = text.split('\n')
    categories = []
    for line in text:
        if re.match(r'\[\[category', line):
            categories.append(re.sub(r'\[\[category:(.*)\]\]', r'\1', line))
    categories = tokenize(' '.join(categories))
    categories = remove_stopwords(categories)
    categories = stem(categories)
    return categories

def process_links(text):
    text = text.split('\n')
    links = []
    for line in text:
        if re.match(r'\*[\ ]*\[', line):
            links.append(line)
    links = tokenize(' '.join(links))
    links = remove_stopwords(links)
    links = stem(links)
    return links

def indexer(title, body, info, categories, links):
    global pg_cnt, posting_list, doc_id, f_cnt, offset
    id_ = pg_cnt
    terms={}

    # Doc vocab
    dic={}
    for term in links: 
        if(dic.get(term)==None):
            dic[term]=1
        else:
            dic[term]+=1
        if(terms.get(term)==None):
            terms[term]=1
        else:
            terms[term]+=1
    links = dic

    dic = {} 
    for term in title:
        if(dic.get(term)==None):
            dic[term]=1
        else:
            dic[term]+=1
        if(terms.get(term)==None):
            terms[term]=1
        else:
            terms[term]+=1
    title = dic

    dic = {}
    for term in info:
        if(dic.get(term)==None):
            dic[term]=1
        else:
            dic[term]+=1
        if(terms.get(term)==None):
            terms[term]=1
        else:
            terms[term]+=1
    info = dic

    dic = {}
    for term in body:
        if(dic.get(term)==None):
            dic[term]=1
        else:
            dic[term]+=1
        if(terms.get(term)==None):
            terms[term]=1
        else:
            terms[term]+=1
    body = dic

    dic = {}
    for term in categories:
        if(dic.get(term)==None):
            dic[term]=1
        else:
            dic[term]+=1
        if(terms.get(term)==None):
            terms[term]=1
        else:
            terms[term]+=1
    categories = dic 

    for term,key in terms.items():
    	string ='d'+(str(id_))
    	if(title.get(term)):
    		string += 't' + str(title[term])
    	if(body.get(term)):
    		string += 'b' + str(body[term])
    	if(info.get(term)):
    		string += 'i' + str(info[term])
    	if(categories.get(term)):
    		string += 'c' + str(categories[term])
    	if(links.get(term)):
    		string += 'l' + str(links[term])
    	posting_list[term].append(string)
    pg_cnt += 1
    if pg_cnt%25000 == 0 :
    	offset = write_temp_file(posting_list, doc_id, f_cnt , offset)
    	posting_list = defaultdict(list)
    	doc_id = {}
    	f_cnt = f_cnt + 1

def write_temp_file(posting_list, doc_id, f_cnt , offset):	
    # Initialize with empty lists
    titles_offset = []    
    titles        = []
    pg_offset = offset  # will return pg_offset

    for key in sorted(doc_id):
        temp = str(key) + ' ' + doc_id[key].strip()
        if(  len(temp) > 0  ):
            pg_offset += 1 + len(temp)
        else:
            pg_offset += 1
        titles.append(temp)
        titles_offset.append(str(pg_offset))

    f_path = os.path.join (indices_folder, 'titleOffset.txt')
    f =  open(f_path, 'a')
    f.write('\n'.join(titles_offset))
    f.write('\n')

    f_path = os.path.join (indices_folder, 'title.txt') 
    f =  open(f_path, 'a')
    f.write('\n'.join(titles))
    f.write('\n')

    data = []
    for key in sorted(posting_list.keys()):
        postings = posting_list[key]
        string = key + ' '
        string = string + ' '.join(postings)
        data.append(string)

    temp_folder = os.path.join(indices_folder, 'temp')
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    f_path = os.path.join(temp_folder, 'tempfile' + str(f_cnt) + '.txt')
    f      =  open(f_path, 'w')
    f.write('\n'.join(data))
    return pg_offset

class WriteThread(threading.Thread):
    def __init__(self, field, data, offset, count):
        threading.Thread.__init__(self)
        self.field = field
        self.offset = offset
        self.data = data
        self.count = count
    def run(self):
        fl_cnt=str(self.count)

        f_path = os.path.join(indices_folder, self.field + fl_cnt + '.txt') 
        f = open(f_path, 'w')
        f.write('\n'.join(self.data))

        f_path = os.path.join(indices_folder, 'offset_' + self.field + fl_cnt + '.txt') 
        f = open(f_path, 'w')
        f.write('\n'.join(self.offset))

def write_indices(data, total_count, offset_len):
    # Initialize empty
    offset         = []
    distinct_terms = []
    title    = defaultdict(dict)
    link     = defaultdict(dict)
    info     = defaultdict(dict)
    category = defaultdict(dict)
    body     = defaultdict(dict)

    for key in tqdm(sorted(data.keys())):
        # Initialize empty after every loop run
        temp    = []
        docs    = data[key]
        num_docs= len(docs)
        i       = 0
        while(i<num_docs): # loop will run for each posting
            posting = docs[i]
            doc_id = re.sub(r'.*d([0-9]*).*', r'\1', posting)

            temp = re.sub(r'.*c([0-9]*).*', r'\1', posting)

            if len(temp)>0 and posting != temp:
                category[key][doc_id] = float(temp)
            temp = re.sub(r'.*i([0-9]*).*', r'\1', posting)

            if len(temp)>0 and posting != temp:
                info[key][doc_id] = float(temp)
            temp = re.sub(r'.*l([0-9]*).*', r'\1', posting)

            if len(temp) > 0 and posting != temp:
                link[key][doc_id] = float(temp)
            temp = re.sub(r'.*b([0-9]*).*', r'\1', posting)

            if len(temp)>0 and posting != temp:
                body[key][doc_id] = float(temp)
            temp = re.sub(r'.*t([0-9]*).*', r'\1', posting)

            if len(temp) >0 and posting != temp:
                title[key][doc_id] = float(temp)
            i+=1

        string = key + ' ' + str(total_count) + ' ' + str(len(docs))   # term  which-file   doc_freq
        offset.append(str(offset_len))
        offset_len += len(string) + 1
        distinct_terms.append(string)    #  for vocab

    category_data = []
    body_data     = []
    link_data     = []
    info_data     = []
    title_data    = []

    category_offset = []
    body_offset     = []
    link_offset     = []
    info_offset     = []
    title_offset    = []

    prev_category = 0
    prevBody      = 0
    prev_link     = 0
    prev_info     = 0
    prev_title    = 0

    for key in tqdm(sorted(data.keys())):
        if key in link:
            docs = link[key]
            docs = sorted(docs, key = docs.get, reverse=True)  #sorted in decreasing order of freq of val of key
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(link[key][doc]) + ' '
            link_data.append(string)
            link_offset.append(str(prev_link) + ' ' + str(len(docs)))
            prev_link += len(string) + 1

        if key in info:
            docs = info[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(info[key][doc]) + ' '
            info_data.append(string)
            info_offset.append(str(prev_info) + ' ' + str(len(docs)))
            prev_info += len(string) + 1

        if key in body:
            docs = body[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(body[key][doc]) + ' '
            body_data.append(string)
            body_offset.append(str(prevBody) + ' ' + str(len(docs)))
            prevBody += len(string) + 1

        if key in category:
            docs = category[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(category[key][doc]) + ' '
            category_data.append(string)
            category_offset.append(str(prev_category) + ' ' + str(len(docs)))
            prev_category += len(string) + 1

        if key in title:
            docs = title[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(title[key][doc]) + ' '
            title_data.append(string)
            title_offset.append(str(prev_title) + ' ' + str(len(docs)))
            prev_title += len(string) + 1

    thread = []
    thread.append(WriteThread('t', title_data, title_offset, total_count))
    thread.append(WriteThread('b', body_data, body_offset, total_count))
    thread.append(WriteThread('i', info_data, info_offset, total_count))
    thread.append(WriteThread('c', category_data, category_offset, total_count))
    thread.append(WriteThread('l', link_data, link_offset, total_count))

    i=0
    while(i<5):
        thread[i].start()
        i+=1

    i=0
    while(i<5):
        thread[i].join()
        i+=1

    f_path = os.path.join (indices_folder, 'offset.txt' )
    f = open(f_path, 'a')
    f.write('\n'.join(offset))
    f.write('\n')

    f_path = os.path.join (indices_folder, 'vocab.txt' )
    f = open(f_path, 'a')
    f.write('\n'.join(distinct_terms))
    f.write('\n')

    return offset_len , total_count+1
  
def mergefiles(file_count):
    # Initialize empty
    check_files  = [0] * file_count
    heap  = []
    terms, files, top = {}, {}, {}
    total_count,offset_len = 0,0
    data = defaultdict(list)

    i=0
    while i < file_count:
        f_path   = os.path.join(indices_folder, 'temp', 'tempfile' + str(i) + '.txt')
        files[i] = open(f_path, 'r')
        top[i]   = files[i].readline().strip()   # first line of file = term followed by its posting lists
        terms[i] = top[i].split()                # term followed by its posting lists in first line
        t1 = terms[i][0]                         # first term of first line in file 
        if t1 not in heap:
            heapq.heappush(heap,t1)              # Insert in heap - first word of EVERY file
        check_files[i] = 1
        i=i+1

    count = 0
    while any(check_files) == 1:    # any -returns True if any item in an iterable are true, else it returns False
        temp = heapq.heappop(heap)  # removes and returns smallest from heap
        count += 1                  # count of words handled till now

        if count%100000 == 0:       # will repeat after 1 lakh count
            oldFileCount = total_count
            offset_len,total_count = write_indices(data, total_count, offset_len) # data will be filled below
            if total_count != oldFileCount :     # more files to be written
                data = defaultdict(list)         # re-initialize
        i=0
        while i < file_count:      # this loop will run for each 'temp' - searching in each file
            if check_files[i]:
                if temp == terms[i][0] :
                    top[i] = files[i].readline().strip()   # read the next line of ith file
                    data[temp].extend(terms[i][1:])        # add the posting list of temp from ith file
                    if top[i]!='':                         # is not empty
                        terms[i] = top[i].split()          # Convert to list i.e. same as above
                        if terms[i][0] not in heap:        # Insert in heap - same as above
                            heapq.heappush(heap, terms[i][0])
                    else:                                  # Is empty - i.e. reached end of file
                        check_files[i] = 0                 # No need of checking in that file anymore
                        files[i].close()                   # So close that file
            i+=1            
    offset_len, total_count = write_indices(data, total_count, offset_len)
    
class Wiki_Xml_Handle(xml.sax.ContentHandler):
    def __init__(self):
        self.title = ''
        self.text = ''
        self.data = ''
        self.id = ''
        self.fl = 0
    def startElement(self, tag, attributes):
        self.data = tag   
    def endElement(self, tag):
        if tag == 'page':
            doc_id[pg_cnt] = self.title.strip().encode("ascii", errors="ignore").decode()
            title, body, info, categories, links = process_text(self.text, self.title)
            indexer( title, body, info, categories, links)
            self.data = ''
            self.title = ''
            self.text = ''
            self.id = ''
            self.fl = 0
            #print('Finished:',pg_cnt)
    def characters(self, content):
        if self.data == 'title':
            self.title = self.title + content
        elif self.data == 'id' and self.fl == 0:
            self.id = content
            self.fl = 1
        elif self.data == 'text':
            self.text += content

class Parser():
    def __init__(self, file_input):
        self.parser = xml.sax.make_parser()
        self.parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        self.handler = Wiki_Xml_Handle()
        self.parser.setContentHandler(self.handler)
        self.parser.parse(file_input)

if __name__ == '__main__':

    start = time()
    xml_folder = sys.argv[1]
    #index_addr = sys.argv[2]

    # Store stop words in a dictionary
    stops = {}
    for word in stopwords:
    	stops[word] = True

    doc_id = {} ## {Doc id : Title}
    pg_cnt = 0 ### Page Count
    f_cnt = 0 ### File Count
    offset = 0 ## Offset
    posting_list = defaultdict(list)
    ##### Initialise Porter Stemmer
    #stemmer = PorterStemmer() 
    stemmer = Stemmer.Stemmer('english')

    # xml Parsing
    xml_files = os.listdir(xml_folder)
    for xml_file in xml_files:
        start_loop = time()
        print (xml_file, strftime("%H:%M:%S"), 'started')
        xml_addr = os.path.join(xml_folder, xml_file)
        parser = Parser(xml_addr)
        print (xml_file, strftime("%H:%M:%S"), int(time()-start_loop), 'sec')

    f = open(  os.path.join(indices_folder, 'total_docs.txt' ), 'w')
    f.write(str(pg_cnt))

    offset = write_temp_file(posting_list, doc_id, f_cnt , offset)
    posting_list = defaultdict(list)
    doc_id = {}
    f_cnt = f_cnt + 1
    mergefiles(f_cnt)

    print ('Inverted Index files saved at', indices_folder , int(time()-start), 'seconds')
