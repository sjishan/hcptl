from elasticsearch import Elasticsearch
import ast


#Clean the index if it exist
def clean_index(es, index_name = "events"):
    if es.indices.exists(index= index_name):
        print "Index exist: Deletion of index in process"
        es.indices.delete(index= index_name, ignore=[400, 404])
    else:
        print "Index does not exist"



#Create new index and mapping
def new_index_mapping(es, index_name = "events", document_name = "event"):
    mapping = {
                "event": {
                    "properties": {
                        "created_at": {"type": "date"},
                        "team": {"type": "keyword"},
                        "repo": {"type": "keyword"},
                        "event": {"type": "keyword"}
                                    }
                                        }
                                            }
    
    if not es.indices.exists(index_name):
        es.indices.create(index_name)
        print "New index created"
    
    map_check = es.indices.put_mapping(index=index_name,doc_type=document_name,body=mapping)
    
    if map_check["acknowledged"]:
        print "New mapping created"


#Clean the event row
def set_row(row, index_name = "events", document_name = "event"):
    row_dict = ast.literal_eval(row)
    
    row_dict_clean = {
        "_index": index_name,
        "_type": document_name,
        "_source": {
        'created_at': row_dict['created_at'],
        'team' : row_dict['org']['login'],
        'repo' : row_dict['repo']['name'].split('/')[1],
        'event' : row_dict['type']
        }
    }
    
    return row_dict_clean