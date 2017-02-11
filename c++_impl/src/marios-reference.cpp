#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <sstream>
#include <unordered_set>



using namespace std;

unordered_set<string> ngramsMap;


int query1 = 1;;

class Node {
public:
    Node() { mContent = ' '; mMarker = false; }
    ~Node() {}
    char content() { return mContent; }
    void setContent(char c) { mContent = c; }
    bool wordMarker() { return mMarker; }
    void setWordMarker(bool value) { mMarker = value; }
    Node* findChild(char c);
    void appendChild(Node* child) { mChildren.push_back(child); }
    vector<Node*> children() { return mChildren; }

private:
    char mContent;
    bool mMarker;
    vector<Node*> mChildren;
};

class Trie {
public:
    Trie();
    ~Trie();
    void addWord(string&& s);
    Node* searchWord(string s);
    void deleteWord(string s);
private:
    Node* root;
};

Node* Node::findChild(char c)
{
    for ( int i = 0; i < mChildren.size(); i++ )
    {
       // Node* tmp = mChildren[i];
        if ( mChildren[i]->content() == c )
        {
            return mChildren[i];
        }
    }

    return NULL;
}

Trie::Trie()
{
    root = new Node();
}

Trie::~Trie()
{
    // Free memory
}

void Trie::addWord(string&& s)
{
    Node* current = root;

    if ( s.length() == 0 )
    {
        current->setWordMarker(true); // an empty word
        return;
    }

    for ( int i = 0; i < s.length(); i++ )
    {
        Node* child = current->findChild(s[i]);
        if ( child != NULL )
        {
            current = child;
        }
        else
        {
            Node* tmp = new Node();
            tmp->setContent(s[i]);
            current->appendChild(tmp);
            current = tmp;
        }
        if ( i == s.length() - 1 )
            current->setWordMarker(true);
    }
}

void Trie::deleteWord(string s)
{
	Node* current = root;

	    while ( current != NULL )
	    {
	        for ( int i = 0; i < s.length(); i++ )
	        {
	            Node* tmp = current->findChild(s[i]);
	            if ( tmp == NULL ){

	                return ;
	            }
	            current = tmp;
	        }
	        current->setWordMarker(false);
		return ;

	    }

}


Node* Trie::searchWord(string s)
{
    Node* current = root;

    while ( current != NULL )
    {
        for ( int i = 0; i < s.length(); i++ )
        {
            Node* tmp = current->findChild(s[i]);
            if ( tmp == NULL )
                return NULL;
            current = tmp;
        }

	return current;
    }

    return NULL;
}

Trie* trie = new Trie();

string query(string document)
{
	unsigned docLength = document.length();
	unordered_set<string> checkedWords;
	std::string result= "";
	//doclength -1
	string word = "";
	int index = 1;
	int indexChar =0;
	bool firstTime = true;
	int countWords = 0;
	
	
	bool lastTime=false;
	for(unsigned i =0 ; i<docLength ; i++)
	{
		//indexChar=i;
		//query 50

		if(document[i] == ' ' || i==docLength-1)
		{

			if(i==docLength -1 && document[i] != ' ')
			{
				word +=document[i];
			}

			countWords++;
			

			//if hello does not have pointers..
			//then continue

			if(checkedWords.find(word) == checkedWords.end()){

				checkedWords.insert(word);
				Node *nd = trie->searchWord(word);
				if(nd != NULL && nd->wordMarker()){// trie->searchWord(word)){

					result+=word+"|";

				}
				else{
				

					if(firstTime )
					{


						if(i!=docLength-1)
						{
						//just check next childern char

						char nextChar = document[i+1];
						if(nd != NULL)
						{
							Node * space =nd->findChild(' ');
							if( space!=NULL)
							{
								if (space->findChild(nextChar)==NULL)	
								{
								countWords=0;
								firstTime = true;
								
								word="";
								continue;
								}		
							}
							else{

							countWords=0;
								firstTime = true;
								
								word="";
								continue;
							}
						}
						else if(nd == NULL || nd->children().size()==0 || nd->findChild(nextChar)==NULL ){
						countWords=0;
						firstTime = true;
								
						word="";
						continue;
						}

						}
					
						else
						{
						lastTime=true;
						}
					}
					
					

				}
				
				
			}
			else{

				Node *nd = trie->searchWord(word);

				
					if(firstTime )
					{


						if(i!=docLength-1)
						{
						//just check next childern char

						char nextChar = document[i+1];
						if(nd != NULL)
						{
							Node * space =nd->findChild(' ');
							if( space!=NULL)
							{
								if (space->findChild(nextChar)==NULL)	
								{
								countWords=0;
								firstTime = true;
								
								word="";
								continue;
								}		
							}
							else{

							countWords=0;
								firstTime = true;
								
								word="";
								continue;
							}
						}
						else if(nd == NULL || nd->children().size()==0 || nd->findChild(nextChar)==NULL ){
						countWords=0;
						firstTime = true;
								
						word="";
						continue;
						}

						}
					
						else
						{
						lastTime=true;
						}
					}


			}

			
			if(firstTime )
			{
				if(i!=docLength-1){
				indexChar = i;
				firstTime = false;
				}
				else{
				lastTime=true;
				}
				
			}


			if(lastTime)
			break;
			
		/*
			if(countWords == largestGramLength)
			{
				cerr<<"reached"<<endl;

				countWords=0;
				firstTime = true;
				//mporeis na kameis substring dame tse a paeis ws tsiame p theleis anw.
				word="";
				i= indexChar;
				if(i==docLength)
				break;
			}
			else{

*/
				/*if(query1==50)
				{
				cerr<<"found space but c<largests : "<<word<<endl;
				}
				*/
				if (i==docLength-1)
				{
					countWords=0;
									firstTime = true;
									//mporeis na kameis substring dame tse a paeis ws tsiame p theleis anw.
									word="";
									i= indexChar;
									if(i==docLength)
									break;
				}
				else{
					if(i==docLength)
						break;
					word +=document[i];
				}
			//}


		}
		else{
			if(i==docLength)
				break;
			word +=document[i];
			
			/*
			if(checkedWords.find(word) == checkedWords.end()){

				

				checkedWords.insert(word);
				Node *nd = trie->searchWord(word);
				if(nd != NULL && nd->wordMarker()){// trie->searchWord(word)){

					result+=word+"|";

				}
			}
			*/
			//todo: mporeis na fieis to -1
			if(i!=docLength-1){
			//document[i+1]
			Node *nd = trie->searchWord(word);
			if (nd==NULL){//nd->findChild(document[i])==NULL)	{

			countWords=0;
			
				if(!firstTime){
				i= indexChar;
				firstTime = true;
				}
				else{
				//find next space or end

				while(i<docLength)
				{
					i++;
					if(document[i]==' '  ){
					i--;
					break;
					}	
				}

				}				
			word="";
			
			}
			
			
			}


		}
	}

	if(result.length()>1){
	result.pop_back();
	return result;
	}
	else{
	return "-1";
	}

}

// Test program
int main()
{

	
    	int counter=1;
    	 cerr<<"In main"<<endl;
//ngramsMap.reserve(10000);
    	//read initial ngrams
    	for (std::string line; std::getline(std::cin, line) && line != "S";) {
		
		//ngramsMap.insert(std::move(line));
    		trie->addWord(std::move(line));

    	   }
      	  
    	   std::cout << "R" << std::endl;


    	   // Parse input
    	      for (std::string line; std::getline(std::cin, line);) {
    	         if (line == "F") {
    	            std::cout << std::flush;
    	            continue;
    	         }

    	         switch (line[0]) {
    	            case 'Q': {

    	            	string result = query(line.substr(2));
//    	               auto result = ngramer.query(line.substr(2));
//    	               auto ordered = orderResult(std::move(result));
//
//    		      	//cerr<<"query["<<counter<<"]"<<endl;
//                            counter++;
    	               std::cout << result << std::endl;
    	               query1++;
//cerr<<"query "<<query1<<endl;
    	               break;
    	            }
    	            case 'A': {

		

    	            	//TODO: MOVE?
			//ngramsMap.insert(line.substr(2));
    	            	trie->addWord(line.substr(2));
    	            //   ngramer.add(line.substr(2));
    	               break;
    	            }
    	            case 'D': {

			//ngramsMap.erase(line.substr(2));

    	            	
    	            	trie->deleteWord(line.substr(2));
    	            //   ngramer.remove(line.substr(2));
    	               break;
    	            }
    	            default: {
    	               std::cerr << "Error unrecognized line: \"" << line << "\"" << std::endl;
    	               return 1;
    	            }
    	         }
    	      }


    	    return 0;
    }

