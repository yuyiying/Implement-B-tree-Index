/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <limits.h>

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
    vector<SelCond> sortedcond;
    vector<SelCond>::iterator it;
    it = sortedcond.begin();
    
    // sort the condition vetor
    int condcount = 0;
    for(unsigned i = 0; i<cond.size();i++)
    {
        if(cond[i].attr == 1 && cond[i].comp != SelCond::NE)
        {
            sortedcond.insert(it, cond[i]);
            condcount++;
            it = sortedcond.begin();
        }
        else
        {
            sortedcond.push_back(cond[i]);
        }
    }
    
    
    BTreeIndex indexfile;
    
    IndexCursor initial_cursor;
    initial_cursor.pid = 1;
    initial_cursor.eid = 1;
    
    IndexCursor end_cursor;
    end_cursor.pid = indexfile.GetLfEpid();
    end_cursor.eid = indexfile.GetKeycount(end_cursor.pid);
    int leftvalue = INT_MIN;
    int rightvalue = INT_MAX;
    
    if((condcount > 0) && (indexfile.open(table+".idx", 'r')>=0))
    {
        for (int i = 0; i < condcount; i++)
        {
            if(sortedcond[0].comp == SelCond::EQ)
            {
                int value = atoi(sortedcond[0].value);
                if(value <= leftvalue || value >= rightvalue)
                {
                    return  RC_NO_SUCH_RECORD;
                }
                indexfile.locate(value, initial_cursor);
                indexfile.locate(value+1, end_cursor);
                leftvalue = value;
                rightvalue = value+1;
            }
            else if(sortedcond[0].comp == SelCond::LT)
            {
                int value = atoi(sortedcond[0].value);
                if(value < rightvalue)
                {
                    indexfile.locate(value, end_cursor);
                    rightvalue = value;
                }
            }
            else if(sortedcond[0].comp == SelCond::LE)
            {
                int value = atoi(sortedcond[0].value);
                if(value < rightvalue)
                {
                    indexfile.locate(value+1, end_cursor);
                    rightvalue = value;
                }
            }
            else if(sortedcond[0].comp == SelCond::GT)
            {
                int value = atoi(sortedcond[0].value);
                if(value >= leftvalue)
                {
                    indexfile.locate(value+1, initial_cursor);
                    leftvalue = value+1;
                }
            }
            else
            {
                int value = atoi(sortedcond[0].value);
                if(value > leftvalue)
                {
                    indexfile.locate(value, initial_cursor);
                    leftvalue = value;
                }
            }
        }
        int num;
        int   diff;
        int key;
        RecordId rid;
        RC rc;
        string value;
        RecordFile rf;
        if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
            fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
            return rc;
        }
        
        while(initial_cursor.pid != end_cursor.pid || initial_cursor.eid != end_cursor.eid)
        {
            indexfile.readForward(initial_cursor,key,rid);
            
            if ((rc = rf.read(rid, key, value)) < 0) {
                fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                goto exit_select;
            }
            
            for (unsigned i = 1; i < sortedcond.size(); i++) {
                // compute the difference between the tuple value and the condition value
                switch (sortedcond[i].attr) {
                    case 1:
                        diff = key - atoi(sortedcond[i].value);
                        break;
                    case 2:
                        diff = strcmp(value.c_str(), sortedcond[i].value);
                        break;
                }
                
                // skip the tuple if any condition is not met
                switch (sortedcond[i].comp) {
                    case SelCond::EQ:
                        if (diff != 0) goto next_tuple;
                        break;
                    case SelCond::NE:
                        if (diff == 0) goto next_tuple;
                        break;
                    case SelCond::GT:
                        if (diff <= 0) goto next_tuple;
                        break;
                    case SelCond::LT:
                        if (diff >= 0) goto next_tuple;
                        break;
                    case SelCond::GE:
                        if (diff < 0) goto next_tuple;
                        break;
                    case SelCond::LE:
                        if (diff > 0) goto next_tuple;
                        break;
                }
            }
            // the condition is met for the tuple.
            // increase matching tuple counter
            num++;
            
            // print the tuple
            switch (attr) {
                case 1:  // SELECT key
                    fprintf(stdout, "%d\n", key);
                    break;
                case 2:  // SELECT value
                    fprintf(stdout, "%s\n", value.c_str());
                    break;
                case 3:  // SELECT *
                    fprintf(stdout, "%d '%s'\n", key, value.c_str());
                    break;
            }
            
            // move to the next tuple
        next_tuple:
            if(initial_cursor.eid > indexfile.GetKeycount(initial_cursor.pid))
            {
                initial_cursor.eid = 1;
                initial_cursor.pid = indexfile.GetNextpid(initial_cursor.pid);
            }
        }
        
        if (attr == 4) {
            fprintf(stdout, "%d\n", num);
        }
        rc = 0;
        
    exit_select:
        rf.close();
        return rc;
        
        
    }
    else
    {
        RecordFile rf;   // RecordFile containing the table
        RecordId   rid;  // record cursor for table scanning
        
        RC     rc;
        int    key;
        string value;
        int    count;
        int    diff;
        
        // open the table file
        if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
            fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
            return rc;
        }
        
        // scan the table file from the beginning
        rid.pid = rid.sid = 0;
        count = 0;
        while (rid < rf.endRid()) {
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
                fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                goto exit1_select;
            }
            
            // check the conditions on the tuple
            for (unsigned i = 0; i < cond.size(); i++) {
                // compute the difference between the tuple value and the condition value
                switch (cond[i].attr) {
                    case 1:
                        diff = key - atoi(cond[i].value);
                        break;
                    case 2:
                        diff = strcmp(value.c_str(), cond[i].value);
                        break;
                }
                
                // skip the tuple if any condition is not met
                switch (cond[i].comp) {
                    case SelCond::EQ:
                        if (diff != 0) goto next1_tuple;
                        break;
                    case SelCond::NE:
                        if (diff == 0) goto next1_tuple;
                        break;
                    case SelCond::GT:
                        if (diff <= 0) goto next1_tuple;
                        break;
                    case SelCond::LT:
                        if (diff >= 0) goto next1_tuple;
                        break;
                    case SelCond::GE:
                        if (diff < 0) goto next1_tuple;
                        break;
                    case SelCond::LE:
                        if (diff > 0) goto next1_tuple;
                        break;
                }
            }
            
            // the condition is met for the tuple.
            // increase matching tuple counter
            count++;
            
            // print the tuple
            switch (attr) {
                case 1:  // SELECT key
                    fprintf(stdout, "%d\n", key);
                    break;
                case 2:  // SELECT value
                    fprintf(stdout, "%s\n", value.c_str());
                    break;
                case 3:  // SELECT *
                    fprintf(stdout, "%d '%s'\n", key, value.c_str());
                    break;
            }
            
            // move to the next tuple
        next1_tuple:
            ++rid;
        }
        
        // print matching tuple count if "select count(*)"
        if (attr == 4) {
            fprintf(stdout, "%d\n", count);
        }
        rc = 0;
        
        // close the table file and return
    exit1_select:
        rf.close();
        return rc;
    }

}


RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
    /* your code here */
    RecordFile rf;
    RecordId  rid;
    RC rc;
    
    int key;
    string value;
    ifstream in(loadfile.c_str());
    
    if ((rc = rf.open(table + ".tbl", 'w')) < 0) {
        fprintf(stderr, "Open table failed!\n");
        return rc;
    }
    
    string line;
    if(in)
    {
        if(index)
        {
            BTreeIndex  tableindex;
            tableindex.open(table+ ".idx" ,'w');
            while(getline(in,line))
            {
                parseLoadLine(line,key,value);
                rf.append(key,value,rid);
                tableindex.insert(key,rid);
            }
            tableindex.close();
        }
        else
        {
            while(getline(in,line))
            {
                parseLoadLine(line,key,value);
                rf.append(key,value,rid);
            }
        }
        
    }
    else
    {
        fprintf(stderr, "Open loadfile failed!\n");
    }
    
    in.close();
    rf.close();
    return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
