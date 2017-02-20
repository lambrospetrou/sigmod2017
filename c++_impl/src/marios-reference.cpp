#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <sstream>
#include <unordered_set>
#include <unordered_map>



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
    string contents;

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
    void deleteWord(string&& s);
    Node* root;
};

Node* Node::findChild(char c)
{
    std::size_t found = contents.find(c);
    if(found!=string::npos)
    {
	return mChildren[found];
	
    }

    return NULL;

/*
    for ( int i = 0; i < mChildren.size(); i++ )
    {
       // Node* tmp = mChildren[i];
        if ( mChildren[i]->content() == c )
        {
            return mChildren[i];
        }
    }


    return NULL;
*/
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
	    current->contents.push_back(s[i]);
            current = tmp;
        }
        if ( i == s.length() - 1 )
        current->setWordMarker(true);
           
    }
     
}

//u need to fix this.
void Trie::deleteWord(string&& s)
{
	Node* current = root;
	//Node* prev = root;
	int length=s.length();
	    while ( current != NULL )
	    {
	        for ( int i = 0; i < length; i++ )
	        {
	            Node* tmp = current->findChild(s[i]);
	            if ( tmp == NULL ){

	                return ;
	            }
	            current = tmp;
		//  if (i==length-2)
		//   {
		//   prev = tmp;
//
		//   }
	        }
	        //prev->contents[current->content()]
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
	//int largestGramLength = largestGram.first;
	unordered_set<string> checkedWords;

	std::string result= "-1 ";
	result.reserve(1000);
	//doclength -1
	string word = "";
	int index = 1;
	int indexChar =0;
	bool savedIndex = false;
	int countWords = 0;
	checkedWords.reserve(1000);

	//int total_words = 0;

	
	bool lastTime=false;
	bool firstTimeParse=false;
	Node * start = trie->root;
	int i =0;

	bool first = true;
	while( i<docLength )
	{	//if(query1 == 1 && i<600 )
		//cerr<<" i "<<i<<" word = "<<word<<endl;
		//resolution
		if(document[i] == ' ' || i==docLength-1)
		{

			if(i==docLength-1 && document[i]!= ' ')
			{
			word+=document[i];
			start = start->findChild(document[i]);
			}

			if(start!=NULL && start!=trie->root)
			{
			
				if(start->wordMarker())
				{	//auto it = checkedWords.find(word);
					if(checkedWords.find(word) == checkedWords.end()){
				
						checkedWords.insert(word);
					if(first)
					{
					result = word+"|";
					first = false;
					}
					else{
					result+=word+"|";
					}
					
					}
					/*else{
					it->second++
					}
		
					*/
					
				}
				
				start = start->findChild(document[i]);
				

				if(start==NULL)
				{
				
				i++;
					start=trie->root;
					word = "";
					if(savedIndex )
			  		{
					//cerr<<" i "<<i<<" now NULL savedindex true "<<indexChar<<endl;
			 		 i=indexChar;
			  		savedIndex=false;

			 		 }
			  		
					
				}
				else{
				
				//cerr<<"space not null"<<endl;
				i++;
				         if(!savedIndex )
			  		{
					indexChar=i;
			  		savedIndex = true;
					}
				
				word.push_back(' ');
				
				continue;
				}

			
			}
			else 
			{
			//cerr<<" i "<<i<<" start = null or start == root "<<endl;
			  start=trie->root;
			  word="";
			  if(savedIndex )
			  {
				//cerr<<" i "<<i<<" start = null or start == root saved index true "<<indexChar<<endl;
			  i=indexChar;
			  savedIndex=false;

			  }
			  else{
			   i++;
			//cerr<<" i "<<i<<" start = null or start == root saved index false "<<endl;
			  indexChar=i;
			  savedIndex = true;
			  }
			  
			}


		}
		//NOT A SPACE
		else
		{
			//cerr<<" i "<<i<<" not a space "<<endl;
			start = start->findChild(document[i]);
			//start = now;

			if(start==NULL)
			{
				//cerr<<" i "<<i<<" now null "<<endl;
				start=trie->root;
				word = "";
				if(savedIndex)
				{
				//cerr<<" i "<<i<<" now null saved index true "<<indexChar<<endl;
				  i=indexChar;
			 	  savedIndex=false;
				


				}
				else{
					//cerr<<" i "<<i<<" now null saved index false "<<indexChar<<endl;
					while(i<docLength)
					{
						
						if(document[i] == ' ')
						{
						
						i++;
						break;
						}
						i++;
					}
				}

			}
			else
			{
			word.push_back(document[i]);
			
			i++;
			}


		}	
		




	}
/*
	for(auto word : checkedWords)
	{
		cerr<<"word: "<<word.first<<" count: "<<word.second<<endl;
		
	}
	cerr<<endl;
*/
	//cerr<<"duplicates "<<total_words<<endl;
	//if(result.length()>1){
	result.pop_back();
	return result;
	//}
	//else{
	//return "-1";
	//}

}

// Test program
int main()
{

	int total_words = 0;
    	int counter=1;

    	for (std::string line; std::getline(std::cin, line) && line != "S";) {
		trie->addWord(std::move(line));
    	   }
      	 	
	   
    	   std::cout << "R" << std::endl;


    	   // Parse input
    	      for (std::string line; std::getline(std::cin, line);) {
    	         if (line == "F") {
    	            std::cout << std::flush;
			//cerr<<endl;
			//cerr<<endl;
    	            continue;
    	         }

    	         switch (line[0]) {
    	            case 'Q': {
			//cerr<<"Q";
			//cerr<<line[0]<<line[1]<<line[2]<<line[3]<<endl;  
			//if(query1 == 1)
			//cerr<<line[1]<<line[2]<<line[3]<<endl;
    	            	//string result =);
			//else{
			//	std::cout<<" SA"<<endl;
			//}
    	               std::cout <<  query(move(line.substr(2))) << endl;
    	               //query1++;
			//if (query1 ==1 )
			
			//cerr<<"query "<<query1<<endl;
    	               break;
    	            }
    	            case 'A': { 
			//cerr<<"A"; 
			//cerr<<"adding : "<<line.substr(2)<<endl;   
			//cerr<<line[0]<<line[1]<<line[2]<<line[3]<<endl;  
    	            	trie->addWord(move(line.substr(2)));
    	    
    	               break;
    	            }
    	            case 'D': {
			//cerr<<"D";
    	            	trie->deleteWord(move(line.substr(2)));
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

