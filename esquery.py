from elasticsearch import Elasticsearch

#Following are the query functions, the parameters are same for all of these. es is elastic search instance, index_name is index name.

def total_number_teams(es, index_name = "events"):
    teams_count =  es.search(
        index = index_name,
        body = {
        "size" : 0,
        "aggs" : {
            "teams_count" : { "cardinality" : { "field" : "team" } }
                }
           }
        )

    print "1. Total number of teams: " + str(teams_count['aggregations']['teams_count']['value'])


def events_repos_teams(es, index_name = "events"):

    teams_repo_count = es.search(
        index = index_name,
        body = {
            "size": 0,
            "aggs": {
                "teams": { "terms": { "field": "team" },
                "aggs": {
                    "repo_count": { "cardinality": { "field": "repo" } }  
                 }
                }  
               }
              }
             )

   
    print "2. Number of active repos in each team: "
    for dict in teams_repo_count['aggregations']['teams']['buckets']:
        print "\tTeam: " + dict['key'] + "\t\tActive repos: " + str(dict['repo_count']['value'] )
        
    print "\n3. Total number of events per team: "
    for dict in teams_repo_count['aggregations']['teams']['buckets']:
        print "\tTeam: " + dict['key'] + "\t\tEvents: " + str(dict['doc_count'] )


def total_number_events(es, index_name = "events"):
    row_counts = es.count( index = index_name )
    print "4. Total Number of events overall: " + str(row_counts['count'])


def event_frequency_overall(es, index_name = "events"):
    
    event_type_count = es.search(
        index = index_name,
        body = {
        "size": 0,
        "aggs": { "event_count": { "terms": { "field": "event" } }  
           } 
      }
        )
    
    print "5. Frequency of event types overall: "
    for dict in event_type_count['aggregations']['event_count']['buckets']:
        print "\tEvent type: " + dict['key'] + "\t\tEvents: " + str(dict['doc_count'])


def event_frequency_team(es, index_name = "events"):
    event_type_count = es.search(
        index = index_name,
        body = {
        "size": 0,
        "aggs": {
            "teams": { "terms": { "field": "team" },
            "aggs": {
                "event_count": { "terms": { "field": "event" } }  
           }
        }  
      }
    }

        )

    print "6. Frequency of event types per team: "
    for dict1 in event_type_count['aggregations']['teams']['buckets']:
        print "\tTeam: " + str(dict1['key'])
        for dict2 in dict1['event_count']['buckets']:
            print "\t\tEvent type: " + dict2['key'] + "\t\tEvents: " + str(dict2['doc_count'])




def event_frequency_repo(es, index_name = "events"):
    event_type_count = es.search(
        index = index_name,
        body = {
        "size": 0,
        "aggs": {
            "repos": { "terms": { "field": "repo" },
            "aggs": {
                "event_count": { "terms": { "field": "event" } }  
           }
        }  
      }
    }

        )
    
    print "7. Frequency of event types per repo: "
    for dict1 in event_type_count['aggregations']['repos']['buckets']:
        print "\tRepo: " + str(dict1['key'])
        for dict2 in dict1['event_count']['buckets']:
            print "\t\tEvent type: " + dict2['key'] + "\t\tEvents: " + str(dict2['doc_count'])




def event_time_difference_team(es, index_name = "events"):
    event_time_team = es.search(
        index = index_name,
        body = {
            "size": 0,
            "aggs": {
            "teams": { "terms": { "field": "team" },
            "aggs": {
                "time_stats": { "stats": { "field": "created_at" } }  
           }
        }  
      }
    }
        )

    print "8. Average Time Difference between events per team: "
    for dict in event_time_team['aggregations']['teams']['buckets']:
        print ("\tTeam: " + dict['key'] + "\t\tTime Difference: " + 
           str((dict['time_stats']['max'] - dict['time_stats']['min'])/dict['time_stats']['count']) + " milliseconds")


def same_event_time_difference_team(es, index_name = "events"):
    event_type_count = es.search(
        index = index_name,
        body = {
        "size": 0,
        "aggs": {
            "teams": { "terms": { "field": "team" },
                        
            "aggs": {
                "events": { "terms": { "field": "event" },
                "aggs": {
                    "time_stats": { "stats": { "field": "created_at" } }  
           }
          }
        }  
      }
    }
   }
        )

    print "9. Average Time Difference between same events per team: "
    for dict1 in event_type_count['aggregations']['teams']['buckets']:
        print "\tTeam: " + str(dict1['key'])
        for dict2 in dict1['events']['buckets']:
            print ("\t\tEvent: " + dict2['key'] + "\t\tTime Difference: " + 
               str((dict2['time_stats']['max'] - dict2['time_stats']['min'])/dict2['time_stats']['count']) + " milliseconds")

