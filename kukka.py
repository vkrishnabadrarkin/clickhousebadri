import random
count = random.randint(200,900)
def gen_model_key(a,object_id = count):
    m_key = '12345.'+ str(a)+ '.' + str(object_id)
    return m_key


dic = {
    "first" : gen_model_key(random.choice([1,2,3])),
    "second": [gen_model_key(21), gen_model_key(333), gen_model_key(765)]
}
print(dic)
print (int(('ass')==('as')))
lis = ['hi','ji','tyui','edf','xcv','poiuy','fgh','dc']
poi = random.sample(lis,2)
print (poi)