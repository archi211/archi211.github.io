//+------------------------------------------------------------------+
//|                                                     Free_OTC.mq5 |
//| PocketOption M1 OHLC bridge                                      |
//+------------------------------------------------------------------+
#property version   "2.00"
#property description "Imports PocketOption M1 candles from the local bridge"

input group "Bridge"
input string InpServerUrl          = "http://127.0.0.1:5000";
input string InpApiToken           = "";
input int    InpRequestTimeoutMs   = 2500;
input int    InpPollIntervalMs     = 500;
input int    InpBarsHistory        = 3000;
input int    InpPairRefreshSeconds = 60;

#define JSON_OBJECT 1
#define JSON_ARRAY  2
#define JSON_STRING 3
#define JSON_VALUE  4
#define MAX_PAIRS   256
#define MAX_JSON_DEPTH 32

struct JsonNode
  {
   int type;
   int parent;
   int first_child;
   int last_child;
   int next_sibling;
   string key;
   string value;
  };

struct PairInfo
  {
   string api_symbol;
   string custom_symbol;
   string category;
   string pair_status;
   int digits;
   int failures;
   ulong retry_after_ms;
   bool has_identity;
   string seen_run_id;
   long seen_epoch;
   bool has_candidate;
   string candidate_run_id;
   long candidate_epoch;
   bool needs_full;
   bool has_imported;
   long last_bar_ms;
  };

JsonNode g_json_nodes[];
int g_json_count = 0;
PairInfo g_pairs[];
int g_next_pair_index = 0;
bool g_busy = false;
int g_pair_refresh_failures = 0;
ulong g_pair_refresh_retry_ms = 0;
ulong g_next_pair_refresh_ms = 0;
string g_server_status = "CONNECTING";
string g_last_message = "Waiting for bridge";

void JsonSkipSpace(const string &json,int &pos)
  {
   int length=StringLen(json);
   while(pos<length)
     {
      ushort c=(ushort)StringGetCharacter(json,pos);
      if(c!=' ' && c!='\t' && c!='\r' && c!='\n')
         break;
      pos++;
     }
  }

int JsonHexDigit(const ushort c)
  {
   if(c>='0' && c<='9') return (int)(c-'0');
   if(c>='a' && c<='f') return (int)(c-'a'+10);
   if(c>='A' && c<='F') return (int)(c-'A'+10);
   return -1;
  }

bool JsonReadString(const string &json,int &pos,string &value)
  {
   int length=StringLen(json);
   if(pos>=length || StringGetCharacter(json,pos)!='"')
      return false;

   value="";
   pos++;
   while(pos<length)
     {
      ushort c=(ushort)StringGetCharacter(json,pos++);
      if(c=='"')
         return true;
      if(c<32)
         return false;
      if(c!='\\')
        {
         value+=ShortToString(c);
         continue;
        }
      if(pos>=length)
         return false;

      ushort escaped=(ushort)StringGetCharacter(json,pos++);
      if(escaped=='"' || escaped=='\\' || escaped=='/')
         value+=ShortToString(escaped);
      else if(escaped=='b')
         value+=ShortToString(8);
      else if(escaped=='f')
         value+=ShortToString(12);
      else if(escaped=='n')
         value+=ShortToString(10);
      else if(escaped=='r')
         value+=ShortToString(13);
      else if(escaped=='t')
         value+=ShortToString(9);
      else if(escaped=='u')
        {
         if(pos+4>length)
            return false;
         int code=0;
         for(int i=0;i<4;i++)
           {
            int digit=JsonHexDigit((ushort)StringGetCharacter(json,pos+i));
            if(digit<0)
               return false;
            code=code*16+digit;
           }
         value+=ShortToString((ushort)code);
         pos+=4;
        }
      else
         return false;
     }
   return false;
  }

bool JsonIsNumber(const string &text)
  {
   int length=StringLen(text);
   int pos=0;
   if(length==0)
      return false;
   if(StringGetCharacter(text,pos)=='-')
      pos++;
   if(pos>=length)
      return false;
   if(StringGetCharacter(text,pos)=='0')
      pos++;
   else
     {
      if(StringGetCharacter(text,pos)<'1' || StringGetCharacter(text,pos)>'9')
         return false;
      while(pos<length && StringGetCharacter(text,pos)>='0' && StringGetCharacter(text,pos)<='9')
         pos++;
     }
   if(pos<length && StringGetCharacter(text,pos)=='.')
     {
      pos++;
      int start=pos;
      while(pos<length && StringGetCharacter(text,pos)>='0' && StringGetCharacter(text,pos)<='9')
         pos++;
      if(pos==start)
         return false;
     }
   if(pos<length && (StringGetCharacter(text,pos)=='e' || StringGetCharacter(text,pos)=='E'))
     {
      pos++;
      if(pos<length && (StringGetCharacter(text,pos)=='+' || StringGetCharacter(text,pos)=='-'))
         pos++;
      int start=pos;
      while(pos<length && StringGetCharacter(text,pos)>='0' && StringGetCharacter(text,pos)<='9')
         pos++;
      if(pos==start)
         return false;
     }
   return pos==length;
  }

bool JsonIsInteger(const string &text)
  {
   int length=StringLen(text);
   int pos=0;
   if(length==0)
      return false;
   if(StringGetCharacter(text,pos)=='-')
      pos++;
   if(pos>=length)
      return false;
   if(StringGetCharacter(text,pos)=='0')
      return pos+1==length;
   if(StringGetCharacter(text,pos)<'1' || StringGetCharacter(text,pos)>'9')
      return false;
   for(;pos<length;pos++)
      if(StringGetCharacter(text,pos)<'0' || StringGetCharacter(text,pos)>'9')
         return false;
   return true;
  }

int JsonAddNode(const int type,const int parent,const string key,const string value)
  {
   int index=g_json_count;
   if(index>=ArraySize(g_json_nodes))
      if(ArrayResize(g_json_nodes,index+512,512)<0)
         return -1;
   g_json_nodes[index].type=type;
   g_json_nodes[index].parent=parent;
   g_json_nodes[index].first_child=-1;
   g_json_nodes[index].last_child=-1;
   g_json_nodes[index].next_sibling=-1;
   g_json_nodes[index].key=key;
   g_json_nodes[index].value=value;
   g_json_count++;

   if(parent>=0)
     {
      if(g_json_nodes[parent].first_child<0)
         g_json_nodes[parent].first_child=index;
      else
         g_json_nodes[g_json_nodes[parent].last_child].next_sibling=index;
      g_json_nodes[parent].last_child=index;
     }
   return index;
  }

int JsonParseValue(const string &json,int &pos,const int parent,const string &key,const int depth)
  {
   if(depth>MAX_JSON_DEPTH)
      return -1;
   JsonSkipSpace(json,pos);
   int length=StringLen(json);
   if(pos>=length)
      return -1;

   ushort c=(ushort)StringGetCharacter(json,pos);
   if(c=='{')
     {
      int object=JsonAddNode(JSON_OBJECT,parent,key,"");
      if(object<0) return -1;
      pos++;
      JsonSkipSpace(json,pos);
      if(pos<length && StringGetCharacter(json,pos)=='}')
        {
         pos++;
         return object;
        }
      while(pos<length)
        {
         string child_key;
         if(!JsonReadString(json,pos,child_key)) return -1;
         JsonSkipSpace(json,pos);
         if(pos>=length || StringGetCharacter(json,pos)!=':') return -1;
         pos++;
         if(JsonParseValue(json,pos,object,child_key,depth+1)<0) return -1;
         JsonSkipSpace(json,pos);
         if(pos>=length) return -1;
         c=(ushort)StringGetCharacter(json,pos++);
         if(c=='}') return object;
         if(c!=',') return -1;
         JsonSkipSpace(json,pos);
        }
      return -1;
     }

   if(c=='[')
     {
      int array=JsonAddNode(JSON_ARRAY,parent,key,"");
      if(array<0) return -1;
      pos++;
      JsonSkipSpace(json,pos);
      if(pos<length && StringGetCharacter(json,pos)==']')
        {
         pos++;
         return array;
        }
      while(pos<length)
        {
         if(JsonParseValue(json,pos,array,"",depth+1)<0) return -1;
         JsonSkipSpace(json,pos);
         if(pos>=length) return -1;
         c=(ushort)StringGetCharacter(json,pos++);
         if(c==']') return array;
         if(c!=',') return -1;
        }
      return -1;
     }

   if(c=='"')
     {
      string value;
      if(!JsonReadString(json,pos,value)) return -1;
      return JsonAddNode(JSON_STRING,parent,key,value);
     }

   int start=pos;
   while(pos<length)
     {
      c=(ushort)StringGetCharacter(json,pos);
      if(c==',' || c==']' || c=='}' || c==' ' || c=='\t' || c=='\r' || c=='\n')
         break;
      pos++;
     }
   if(start==pos)
      return -1;
   string primitive=StringSubstr(json,start,pos-start);
   if(primitive!="true" && primitive!="false" && primitive!="null" && !JsonIsNumber(primitive))
      return -1;
   return JsonAddNode(JSON_VALUE,parent,key,primitive);
  }

bool JsonParseDocument(const string &json,int &root)
  {
   g_json_count=0;
   int pos=0;
   root=JsonParseValue(json,pos,-1,"",0);
   if(root<0)
      return false;
   JsonSkipSpace(json,pos);
   return pos==StringLen(json);
  }

int JsonFind(const int object,const string key)
  {
   if(object<0 || object>=g_json_count || g_json_nodes[object].type!=JSON_OBJECT)
      return -1;
   int child=g_json_nodes[object].first_child;
   while(child>=0)
     {
      if(g_json_nodes[child].key==key)
         return child;
      child=g_json_nodes[child].next_sibling;
     }
   return -1;
  }

bool JsonStringValue(const int node,string &value)
  {
   if(node<0 || node>=g_json_count || g_json_nodes[node].type!=JSON_STRING)
      return false;
   value=g_json_nodes[node].value;
   return true;
  }

bool JsonIntegerValue(const int node,long &value)
  {
   if(node<0 || node>=g_json_count || g_json_nodes[node].type!=JSON_VALUE ||
      !JsonIsInteger(g_json_nodes[node].value))
      return false;
   value=StringToInteger(g_json_nodes[node].value);
   return true;
  }

bool JsonNumberValue(const int node,double &value)
  {
   if(node<0 || node>=g_json_count || g_json_nodes[node].type!=JSON_VALUE ||
      !JsonIsNumber(g_json_nodes[node].value))
      return false;
   value=StringToDouble(g_json_nodes[node].value);
   return MathIsValidNumber(value);
  }

int JsonArrayCount(const int array)
  {
   if(array<0 || array>=g_json_count || g_json_nodes[array].type!=JSON_ARRAY)
      return -1;
   int count=0;
   int child=g_json_nodes[array].first_child;
   while(child>=0)
     {
      count++;
      child=g_json_nodes[child].next_sibling;
     }
   return count;
  }

int JsonArrayAt(const int array,const int index)
  {
   if(index<0 || array<0 || array>=g_json_count || g_json_nodes[array].type!=JSON_ARRAY)
      return -1;
   int child=g_json_nodes[array].first_child;
   for(int i=0;i<index && child>=0;i++)
      child=g_json_nodes[child].next_sibling;
   return child;
  }

bool JsonProtocolV2(const int root)
  {
   long protocol=0;
   return JsonIntegerValue(JsonFind(root,"protocol"),protocol) && protocol==2;
  }

bool IsPairStatus(const string status)
  {
   return status=="LIVE" || status=="STALE" || status=="LOADING" || status=="UNAVAILABLE";
  }

bool IsValidOtcSymbol(const string symbol)
  {
   int length=StringLen(symbol);
   if(length<5 || length>24)
      return false;
   string lower=symbol;
   StringToLower(lower);
   if(StringSubstr(lower,length-4)!="_otc")
      return false;
   for(int i=0;i<length-4;i++)
     {
      ushort c=(ushort)StringGetCharacter(symbol,i);
      if(!((c>='A' && c<='Z') || (c>='a' && c<='z') || (c>='0' && c<='9') ||
           c=='_' || c=='#' || c=='.' || c=='-'))
         return false;
     }
   return true;
  }

int FindPair(const string api_symbol)
  {
   for(int i=0;i<ArraySize(g_pairs);i++)
      if(g_pairs[i].api_symbol==api_symbol)
         return i;
   return -1;
  }

bool EnsureCustomSymbol(const int index)
  {
   if(index<0 || index>=ArraySize(g_pairs))
      return false;
   string name=g_pairs[index].custom_symbol;
   bool is_custom=false;
   bool created=false;
   if(!SymbolExist(name,is_custom))
     {
      ResetLastError();
      if(!CustomSymbolCreate(name,"",_Symbol))
        {
         int error=GetLastError();
         PrintFormat("Cannot create custom symbol %s (error %d)",name,error);
         return false;
        }
      created=true;
     }
   else if(!is_custom)
     {
      PrintFormat("Symbol %s exists but is not custom",name);
      return false;
     }

   if(created)
     {
      // Changing digits on an existing custom symbol clears its price history.
      ResetLastError();
      if(!CustomSymbolSetInteger(name,SYMBOL_DIGITS,g_pairs[index].digits))
        {
         PrintFormat("Cannot set digits for %s (error %d)",name,GetLastError());
         return false;
        }
     }
   else if(SymbolInfoInteger(name,SYMBOL_DIGITS)!=(long)g_pairs[index].digits)
     {
      PrintFormat("Digits mismatch for %s; refusing to alter existing history",name);
      return false;
     }
   ResetLastError();
   if(!SymbolSelect(name,true))
     {
      PrintFormat("Cannot select %s (error %d)",name,GetLastError());
      return false;
     }
   return true;
  }

bool ParsePairList(const string &body,PairInfo &fresh[])
  {
   int root=-1;
   if(!JsonParseDocument(body,root) || g_json_nodes[root].type!=JSON_OBJECT || !JsonProtocolV2(root))
      return false;
   string run_id,server_status;
   if(!JsonStringValue(JsonFind(root,"run_id"),run_id) || StringLen(run_id)==0 ||
      !JsonStringValue(JsonFind(root,"status"),server_status) || StringLen(server_status)==0)
      return false;
   int data=JsonFind(root,"data");
   int count=JsonArrayCount(data);
   if(count<0 || count>MAX_PAIRS)
      return false;

   ArrayResize(fresh,0);
   for(int i=0;i<count;i++)
     {
      int item=JsonArrayAt(data,i);
      if(item<0 || g_json_nodes[item].type!=JSON_OBJECT)
         return false;
      string symbol,category,status;
      long digits=0;
      if(!JsonStringValue(JsonFind(item,"symbol"),symbol) || !IsValidOtcSymbol(symbol) ||
         !JsonStringValue(JsonFind(item,"category"),category) || StringLen(category)==0 ||
         !JsonIntegerValue(JsonFind(item,"digits"),digits) || digits<0 || digits>10 ||
         !JsonStringValue(JsonFind(item,"status"),status) || !IsPairStatus(status))
         return false;

      string custom=StringSubstr(symbol,0,StringLen(symbol)-4)+"_OTCpo";
      if(StringLen(custom)>31)
         return false;
      int slot=-1;
      for(int j=0;j<ArraySize(fresh);j++)
         if(fresh[j].api_symbol==symbol) { slot=j; break; }
      if(slot<0)
        {
         slot=ArraySize(fresh);
         if(ArrayResize(fresh,slot+1)<0)
            return false;
         fresh[slot].api_symbol=symbol;
         fresh[slot].custom_symbol=custom;
         fresh[slot].failures=0;
         fresh[slot].retry_after_ms=0;
         fresh[slot].has_identity=false;
         fresh[slot].seen_run_id="";
         fresh[slot].seen_epoch=0;
         fresh[slot].has_candidate=false;
         fresh[slot].candidate_run_id="";
         fresh[slot].candidate_epoch=0;
         fresh[slot].needs_full=false;
         fresh[slot].has_imported=false;
         fresh[slot].last_bar_ms=0;
        }
      fresh[slot].category=category;
      fresh[slot].pair_status=status;
      fresh[slot].digits=(int)digits;
     }
   g_server_status=server_status;
   return true;
  }

bool ApplyPairList(PairInfo &fresh[])
  {
   for(int i=0;i<ArraySize(fresh);i++)
     {
      int index=FindPair(fresh[i].api_symbol);
      if(index<0)
        {
         if(ArraySize(g_pairs)>=MAX_PAIRS)
            return false;
         index=ArraySize(g_pairs);
         if(ArrayResize(g_pairs,index+1)<0)
            return false;
         g_pairs[index].api_symbol=fresh[i].api_symbol;
         g_pairs[index].custom_symbol=fresh[i].custom_symbol;
         g_pairs[index].category=fresh[i].category;
         g_pairs[index].pair_status=fresh[i].pair_status;
         g_pairs[index].digits=fresh[i].digits;
         g_pairs[index].failures=0;
         g_pairs[index].retry_after_ms=0;
         g_pairs[index].has_identity=false;
         g_pairs[index].seen_run_id="";
         g_pairs[index].seen_epoch=0;
         g_pairs[index].has_candidate=false;
         g_pairs[index].candidate_run_id="";
         g_pairs[index].candidate_epoch=0;
         g_pairs[index].needs_full=false;
         g_pairs[index].has_imported=false;
         g_pairs[index].last_bar_ms=0;
        }
      else
        {
         g_pairs[index].category=fresh[i].category;
         g_pairs[index].pair_status=fresh[i].pair_status;
         g_pairs[index].digits=fresh[i].digits;
        }
      if(!EnsureCustomSymbol(index))
         SchedulePairRetry(index,GetTickCount64(),false);
     }
   return true;
  }

bool ParseRates(const int data,MqlRates &rates[],long &last_bar_ms)
  {
   int count=JsonArrayCount(data);
   if(count<0 || count>3000)
      return false;
   if(ArrayResize(rates,count)<0)
      return false;
   ArraySetAsSeries(rates,false);
   last_bar_ms=0;
   long previous_ms=0;
   int row=g_json_nodes[data].first_child;
   for(int i=0;i<count;i++)
     {
      if(JsonArrayCount(row)!=5)
         return false;
      long time_ms=0;
      double open=0.0,high=0.0,low=0.0,close=0.0;
      if(!JsonIntegerValue(JsonArrayAt(row,0),time_ms) || time_ms<=0 ||
         time_ms%60000!=0 || time_ms>=4102444800000 ||
         !JsonNumberValue(JsonArrayAt(row,1),open) ||
         !JsonNumberValue(JsonArrayAt(row,2),high) ||
         !JsonNumberValue(JsonArrayAt(row,3),low) ||
         !JsonNumberValue(JsonArrayAt(row,4),close) ||
         open<=0.0 || high<=0.0 || low<=0.0 || close<=0.0 ||
         high<open || high<close || high<low || low>open || low>close)
         return false;
      if(i>0 && time_ms<=previous_ms)
         return false;

      rates[i].time=(datetime)(time_ms/1000);
      rates[i].open=open;
      rates[i].high=high;
      rates[i].low=low;
      rates[i].close=close;
      rates[i].tick_volume=0;
      rates[i].spread=0;
      rates[i].real_volume=0;
      previous_ms=time_ms;
      last_bar_ms=time_ms;
      row=g_json_nodes[row].next_sibling;
     }
   return true;
  }

bool ParseKlines(const string &body,string &run_id,long &epoch,string &server_status,
                 string &pair_status,long &last_tick_ms,bool &has_last_tick,
                 bool &history_complete,MqlRates &rates[],long &last_bar_ms)
  {
   int root=-1;
   if(!JsonParseDocument(body,root) || g_json_nodes[root].type!=JSON_OBJECT || !JsonProtocolV2(root))
      return false;
   if(!JsonStringValue(JsonFind(root,"run_id"),run_id) || StringLen(run_id)==0 ||
      !JsonIntegerValue(JsonFind(root,"epoch"),epoch) || epoch<0 ||
      !JsonStringValue(JsonFind(root,"status"),server_status) || StringLen(server_status)==0 ||
      !JsonStringValue(JsonFind(root,"pair_status"),pair_status) || !IsPairStatus(pair_status))
      return false;

   int tick=JsonFind(root,"last_tick_ms");
   has_last_tick=false;
   last_tick_ms=0;
   if(tick<0 || g_json_nodes[tick].type!=JSON_VALUE)
      return false;
   if(g_json_nodes[tick].value!="null")
     {
      if(!JsonIntegerValue(tick,last_tick_ms) || last_tick_ms<0)
         return false;
      has_last_tick=true;
     }
   int complete=JsonFind(root,"history_complete");
   if(complete<0 || g_json_nodes[complete].type!=JSON_VALUE ||
      (g_json_nodes[complete].value!="true" && g_json_nodes[complete].value!="false"))
      return false;
   history_complete=(g_json_nodes[complete].value=="true");
   return ParseRates(JsonFind(root,"data"),rates,last_bar_ms);
  }

bool HttpGet(const string url,string &body,int &http_status,int &error_code)
  {
   char request_data[];
   char response[];
   string response_headers;
   string headers="Accept: application/json\r\n";
   if(StringLen(InpApiToken)>0)
      headers+="Authorization: Bearer "+InpApiToken+"\r\n";
   ArrayResize(request_data,0);
   ResetLastError();
   http_status=WebRequest("GET",url,headers,InpRequestTimeoutMs,request_data,response,response_headers);
   error_code=GetLastError();
   if(http_status<0)
     {
      body="";
      return false;
     }
   body=CharArrayToString(response,0,-1,CP_UTF8);
   return http_status==200;
  }

int PairRetrySeconds(const int failures,const int initial_seconds)
  {
   int delay=initial_seconds;
   for(int i=1;i<failures && delay<60;i++)
      delay*=2;
   if(delay>60) delay=60;
   return delay;
  }

void SchedulePairRetry(const int index,const ulong now_ms,const bool unauthorized)
  {
   if(index<0 || index>=ArraySize(g_pairs))
      return;
   g_pairs[index].failures++;
   if(g_pairs[index].failures>8)
      g_pairs[index].failures=8;
   int delay=unauthorized ? 60 : PairRetrySeconds(g_pairs[index].failures,2);
   g_pairs[index].retry_after_ms=now_ms+(ulong)delay*1000;
  }

void SchedulePairRefreshRetry(const ulong now_ms,const bool unauthorized)
  {
   g_pair_refresh_failures++;
   if(g_pair_refresh_failures>8)
      g_pair_refresh_failures=8;
   int delay=unauthorized ? 60 : PairRetrySeconds(g_pair_refresh_failures,5);
   g_pair_refresh_retry_ms=now_ms+(ulong)delay*1000;
  }

string BaseUrl()
  {
   string base=InpServerUrl;
   while(StringLen(base)>0 && StringGetCharacter(base,StringLen(base)-1)=='/')
      base=StringSubstr(base,0,StringLen(base)-1);
   return base;
  }

void RefreshPairs(const ulong now_ms)
  {
   string body;
   int http_status=-1,error_code=0;
   if(!HttpGet(BaseUrl()+"/v1/pairs",body,http_status,error_code))
     {
      g_last_message=StringFormat("Pairs request failed: HTTP %d, error %d",http_status,error_code);
      SchedulePairRefreshRetry(now_ms,http_status==401 || http_status==403);
      Print(g_last_message);
      return;
     }

   PairInfo fresh[];
   if(!ParsePairList(body,fresh) || !ApplyPairList(fresh))
     {
      g_last_message="Invalid /v1/pairs response; keeping existing pairs";
      SchedulePairRefreshRetry(now_ms,false);
      Print(g_last_message);
      return;
     }
   g_pair_refresh_failures=0;
   g_pair_refresh_retry_ms=0;
   g_next_pair_refresh_ms=now_ms+(ulong)InpPairRefreshSeconds*1000;
   g_last_message=StringFormat("Pair list refreshed (%d configured)",ArraySize(g_pairs));
   Print(g_last_message);
  }

int SelectNextPair(const ulong now_ms)
  {
   int count=ArraySize(g_pairs);
   if(count==0)
      return -1;
   for(int offset=0;offset<count;offset++)
     {
      int index=(g_next_pair_index+offset)%count;
      if(now_ms>=g_pairs[index].retry_after_ms)
        {
         g_next_pair_index=(index+1)%count;
         return index;
        }
     }
   return -1;
  }

string KlinesUrl(const int index,const bool full,const bool probe)
  {
   int limit=InpBarsHistory;
   if(probe) limit=2;
   string encoded_symbol=g_pairs[index].api_symbol;
   StringReplace(encoded_symbol,"#","%23");
   string url=BaseUrl()+"/v1/klines?symbol="+encoded_symbol+
              "&limit="+IntegerToString(limit);
   if(!full && !probe && g_pairs[index].has_imported)
     {
      long since=g_pairs[index].last_bar_ms-60000;
      if(since<0) since=0;
      url+="&since="+IntegerToString(since);
     }
   return url;
  }

bool ImportRates(const int index,MqlRates &rates[],const long expected_last_bar_ms,
                 const bool replace_cursor)
  {
   int expected=ArraySize(rates);
   if(expected==0)
      return false;
   ResetLastError();
   int updated=CustomRatesUpdate(g_pairs[index].custom_symbol,rates,(uint)expected);
   int error=GetLastError();
   if(updated!=expected || error!=0)
     {
      PrintFormat("CustomRatesUpdate %s returned %d/%d (error %d)",
                  g_pairs[index].custom_symbol,updated,expected,error);
      return false;
     }
   if(replace_cursor)
      g_pairs[index].last_bar_ms=expected_last_bar_ms;
   else if(expected_last_bar_ms>g_pairs[index].last_bar_ms)
      g_pairs[index].last_bar_ms=expected_last_bar_ms;
   g_pairs[index].has_imported=true;
   return true;
  }

void ProcessPair(const int index,const ulong now_ms)
  {
   if(!EnsureCustomSymbol(index))
     {
      g_last_message="Custom symbol unavailable: "+g_pairs[index].custom_symbol;
      SchedulePairRetry(index,now_ms,false);
      return;
     }

   bool full=g_pairs[index].needs_full;
   bool probe=!full && !g_pairs[index].has_identity;
   string body;
   int http_status=-1,error_code=0;
   if(!HttpGet(KlinesUrl(index,full,probe),body,http_status,error_code))
     {
      g_last_message=StringFormat("%s request failed: HTTP %d, error %d",
                                  g_pairs[index].api_symbol,http_status,error_code);
      SchedulePairRetry(index,now_ms,http_status==401 || http_status==403);
      Print(g_last_message);
      return;
     }

   string run_id,server_status,pair_status;
   long epoch=0,last_tick_ms=0,last_bar_ms=0;
   bool has_last_tick=false,history_complete=false;
   MqlRates rates[];
   if(!ParseKlines(body,run_id,epoch,server_status,pair_status,last_tick_ms,
                   has_last_tick,history_complete,rates,last_bar_ms))
     {
      g_last_message="Invalid klines response for "+g_pairs[index].api_symbol;
      SchedulePairRetry(index,now_ms,false);
      Print(g_last_message);
      return;
     }

   g_server_status=server_status;
   g_pairs[index].pair_status=pair_status;
   g_pairs[index].failures=0;
   g_pairs[index].retry_after_ms=0;

   bool identity_changed=!g_pairs[index].has_identity ||
                         run_id!=g_pairs[index].seen_run_id ||
                         epoch!=g_pairs[index].seen_epoch;
   if(!full && identity_changed)
     {
      g_pairs[index].candidate_run_id=run_id;
      g_pairs[index].candidate_epoch=epoch;
      g_pairs[index].has_candidate=true;
      g_pairs[index].needs_full=true;
      g_last_message="Bridge generation changed; full history queued for "+g_pairs[index].api_symbol;
      return;
     }

   if(full)
     {
      if(!g_pairs[index].has_candidate || run_id!=g_pairs[index].candidate_run_id ||
         epoch!=g_pairs[index].candidate_epoch)
        {
         g_pairs[index].candidate_run_id=run_id;
         g_pairs[index].candidate_epoch=epoch;
         g_pairs[index].has_candidate=true;
         g_pairs[index].needs_full=true;
         g_last_message="Bridge generation changed during full fetch; retrying "+g_pairs[index].api_symbol;
         return;
        }
      if(ArraySize(rates)==0)
        {
         g_pairs[index].needs_full=true;
         int retry_seconds=(pair_status=="UNAVAILABLE") ? 60 : 2;
         g_pairs[index].retry_after_ms=now_ms+(ulong)retry_seconds*1000;
         g_last_message="Full history is empty; will retry "+g_pairs[index].api_symbol;
         return;
        }
      if(!ImportRates(index,rates,last_bar_ms,true))
        {
         g_pairs[index].needs_full=true;
         SchedulePairRetry(index,now_ms,false);
         g_last_message="Full history import failed for "+g_pairs[index].api_symbol;
         return;
        }
      g_pairs[index].seen_run_id=g_pairs[index].candidate_run_id;
      g_pairs[index].seen_epoch=g_pairs[index].candidate_epoch;
      g_pairs[index].has_identity=true;
      g_pairs[index].has_candidate=false;
      g_pairs[index].needs_full=false;
      if(pair_status=="UNAVAILABLE")
         g_pairs[index].retry_after_ms=now_ms+60000;
      g_last_message=StringFormat("Imported %d bars: %s",
                                  ArraySize(rates),g_pairs[index].custom_symbol);
      return;
     }

   if(!g_pairs[index].has_identity)
     {
      g_pairs[index].candidate_run_id=run_id;
      g_pairs[index].candidate_epoch=epoch;
      g_pairs[index].has_candidate=true;
      g_pairs[index].needs_full=true;
      return;
     }

   if(ArraySize(rates)>0 && !ImportRates(index,rates,last_bar_ms,false))
     {
      SchedulePairRetry(index,now_ms,false);
      g_last_message="Incremental import failed for "+g_pairs[index].api_symbol;
      return;
     }
   if(pair_status=="UNAVAILABLE")
      g_pairs[index].retry_after_ms=now_ms+60000;
   g_last_message=StringFormat("%s %s%s",g_pairs[index].api_symbol,pair_status,
                               has_last_tick ? "" : " (no recent tick)");
  }

void UpdatePanel()
  {
   string text="PocketOption bridge | "+g_server_status+"\n";
   text+=StringFormat("Pairs: %d | %s",ArraySize(g_pairs),g_last_message);
   Comment(text);
  }

int OnInit()
  {
   string base=BaseUrl();
   if((StringFind(base,"http://")!=0 && StringFind(base,"https://")!=0) ||
      InpPollIntervalMs<250 || InpPollIntervalMs>1000 ||
      InpRequestTimeoutMs<250 || InpRequestTimeoutMs>10000 ||
      InpBarsHistory<1 || InpBarsHistory>3000 ||
      InpPairRefreshSeconds<15 || StringFind(InpApiToken,"\r")>=0 || StringFind(InpApiToken,"\n")>=0)
     {
      Print("Invalid bridge configuration. Check URL, token, timer and history limits.");
      return INIT_PARAMETERS_INCORRECT;
     }
   if(!EventSetMillisecondTimer(InpPollIntervalMs))
     {
      PrintFormat("Cannot start timer (error %d)",GetLastError());
      return INIT_FAILED;
     }
   UpdatePanel();
   Print("Free_OTC started. Allow the bridge URL in MT5 Expert Advisors options.");
   return INIT_SUCCEEDED;
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
   Comment("");
  }

void OnTimer()
  {
   if(g_busy)
      return;
   g_busy=true;
   ulong now_ms=GetTickCount64();

   if(now_ms>=g_next_pair_refresh_ms && now_ms>=g_pair_refresh_retry_ms)
     {
      RefreshPairs(now_ms);
      UpdatePanel();
      g_busy=false;
      return;
     }

   int index=SelectNextPair(now_ms);
   if(index>=0)
      ProcessPair(index,now_ms);
   else if(ArraySize(g_pairs)==0)
      g_last_message="Waiting for a valid pair list";
   UpdatePanel();
   g_busy=false;
  }
