/** tamirtf77
 *  
 *  This class implements the IndexWriter.
 */

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class IndexWriter 
{
	private static final String NODES_TXT = "data/nodes.txt";
	private static final String EDGES_TXT = "data/edges.txt";
	private static final String TWEETS_TXT = "data/tweets.txt";
	private static final String DICT_TXT = "data/dict.txt";
	private static final String NAMES_TXT = "data/names.txt";
	
	private static final String DIR_NAME = "index";
	public static final String NODES_INDEX = DIR_NAME + "/nodes_index.txt";
	public static final String FOLLOWING = DIR_NAME + "/following.txt";
	public static final String FOLLOWERS = DIR_NAME + "/followers.txt";
	public static final String TWEETS_INDEX = DIR_NAME + "/tweets_pointers.txt";
	public static final String TWEETS = DIR_NAME + "/tweets_words.txt";
	
	public static final String NAMES_INDEX = DIR_NAME + "/names_index.txt";
	public static final String NAMES_STRING = DIR_NAME + "/names_string.txt";
	public static final String NODES_OF_NAME_POINTERS = DIR_NAME + "/nodes_of_name_pointers.txt";
	public static final String NODES_OF_NAME = DIR_NAME + "/nodes_of_name.txt";
	
	public static final String DICT_INDEX = DIR_NAME + "/dict_index.txt";
	public static final String DICT_STRING = DIR_NAME + "/dict_string.txt";
	public static final String DICT_NODES_POINTERS_TWEETS = DIR_NAME + "/dict_nodes_pointers_to_tweets_numbers.txt";
	public static final String DICT_TWEETS_NUMBERS = DIR_NAME + "/dict_tweets_numbers.txt";
	
	private static final String FOLLOWERS_BLOCK = DIR_NAME + "/followers_block_";
	private static final String NODES_OF_NAMES_BLOCK = DIR_NAME + "/nodes_of_names_block_";
	private static final String DICT_NODES_BLOCK = DIR_NAME + "/dict_nodes_block_";
	private static final String FILE_SUFFIX = ".txt";
	
	public static final int BUFFER_BLOCK =  16777216;//33554432;//16777216;//8388608;//16384;
	public static final int BUFFER_BLOCK2 =  16777216;//33554432;//16777216;//1048576;//16384;
	
	private static final String WORD_SEPERATOR = " ";
	
	private static final byte PAIR = 2;
	private static final byte THREE = 3;
	
	private ArrayList<NameData> _namesData;
	private ArrayList<DictData> _dictData;
	
	private int _numOfNodes,_prevFollowingBytes,_prevFollowersBytes,
	_prevTweetsIndexesBytes,_currentTweetsIndexesBytes,_currentTweetsBytes,
	_nodesNamesRuns,_dictNodesRuns;
	private String[] _wordsOfTweet;
	private FrontCoding _fCoding;
	private BlockOfPairs _nodesNamesBlock;
	private BlockOfThree _dictNodesBlock;

	private int _prevIdForFollowing,_prevIdForFollowers,_prevIdForNames,_prevIdForWords;
	
/**
* Given raw social network data, creates an on disk index
*/
	public void createIndex() 
	{
		createIndexDir();
		_namesData = new ArrayList<NameData>();
		_dictData = new ArrayList<DictData>();
		_fCoding = new FrontCoding();
		_numOfNodes = 0;
		_prevFollowingBytes = 0;
		_prevFollowersBytes = 0;
		_prevIdForFollowing = -1;
		_prevIdForFollowers = -1;
		_prevIdForNames = -1;
		_prevIdForWords = -1;
		_wordsOfTweet = null;
		
		readDictTxt();
		readNamesTxt();
		
		readEdgesTxt();
		readNodesTxt();
		
		writeNamesIndex();
		writeDictIndex();
	}
	
	/**
	* Delete all index files created
	*/
	public void removeIndex() 
	{
		File dir = new File(DIR_NAME);
		if (dir.exists()) 
		{
			for (File f : dir.listFiles()) f.delete();
		}
	}
	
	/** create a directory 'index'. */
	private void createIndexDir()
	{
		File dir = new File(DIR_NAME);
		if (dir.exists()) 
		{
			for (File f : dir.listFiles()) f.delete();
		}
		else
		{
			dir.mkdir();
		}
	}
	
	/** Opens a Scanner to the file which its name is given by fileName. */
	private Scanner openScanner(String fileName)
	{
		Scanner s = null;
		try 
		{
			s = new Scanner(new BufferedReader(new FileReader(fileName)));
		} 
		catch (FileNotFoundException e) {}
		return s;
	}
	
	/** Closes a Scanner. */
	private void closeScanner(Scanner s)
	{
		if (s != null) 
		{
			s.close();
        } 
	}
	
	/** Opens a DataOutputStream to the file which its name is given by fileName. */
	private DataOutputStream openDataOutputStream(String fileName)
	{
		DataOutputStream o = null;
		try 
		{
			o = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
		} 
		catch (FileNotFoundException e) {}
		return o;
	}
	
	/** Closes a DataOutputStream. */
	private void closeDataOutputStream(DataOutputStream o)
	{
    	if (o != null) 
        {
    		try 
            {
				o.close();
			} 
            catch (IOException e) {}
        }
	}
	
	/** Opens a DataOutputStream to the file which its name is given by fileName. */
	private DataInputStream openDataInputStream(String fileName)
	{
		DataInputStream in = null;
		try 
		{
			in = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
		} 
		catch (FileNotFoundException e) {}
		return in;
	}
	
	/** Closes a DataOutputStream. */
	private void closeDataInputStream(DataInputStream in)
	{
    	if (in != null) 
        {
    		try 
            {
				in.close();
			} 
            catch (IOException e) {}
        }
	}
	
	/** Reads nodes.txt and creates an object of NodeData to store the id, name, privacy. */
	private void readNodesTxt()
	{
		DataOutputStream out = openDataOutputStream(NODES_INDEX),
		outFollowing = openDataOutputStream(FOLLOWING),
		outFollowers = openDataOutputStream(FOLLOWERS),
		outTweetsIndexes = openDataOutputStream(TWEETS_INDEX),
		outTweets = openDataOutputStream(TWEETS);
		Scanner inTweets = openScanner(TWEETS_TXT);
		Scanner s = openScanner(NODES_TXT);
		Scanner inFollowing = openScanner(EDGES_TXT);
		DataInputStream inFollowers = openDataInputStream(FOLLOWERS_BLOCK + "1" + FILE_SUFFIX);
		NodeData current = new NodeData();
		int id;
		String name,privacy;
		
		_nodesNamesRuns = 1;
		_nodesNamesBlock = new BlockOfPairs(NODES_OF_NAMES_BLOCK + _nodesNamesRuns + FILE_SUFFIX);
		_dictNodesRuns = 1;
		_dictNodesBlock = new BlockOfThree(DICT_NODES_BLOCK + _dictNodesRuns + FILE_SUFFIX);
		while (s.hasNext()) 
		{
			id = Integer.parseInt(s.next());
            name = Integer.toBinaryString(Integer.parseInt(s.next()));
            privacy = s.next();
            current.init(id, name, privacy);
            readFollowingSorted(current,inFollowing);
            readFollowersSorted(current,inFollowers);
            if (_nodesNamesBlock.add(false,true,BUFFER_BLOCK2,current.getName(), id) == true)
            {
            	_nodesNamesBlock = new BlockOfPairs(NODES_OF_NAMES_BLOCK + (++_nodesNamesRuns) + FILE_SUFFIX);
            	System.gc();
            }
			readTweetsTxt(id,inTweets,outTweetsIndexes,outTweets,current);
			current.writeNodeAndFollows(out,outFollowing,_prevFollowingBytes,outFollowers,_prevFollowersBytes,_prevTweetsIndexesBytes);
			_prevFollowingBytes += current.getSize(FOLLOWING);
			_prevFollowersBytes += current.getSize(FOLLOWERS);
			_prevTweetsIndexesBytes = _currentTweetsIndexesBytes;    
            _numOfNodes++;
        }
		closeScanner(s);
		closeDataInputStream(inFollowers);
		try 
		{
			out.writeInt(_numOfNodes);
		} 
		catch (IOException e) {}
		closeDataOutputStream(out);
		closeDataOutputStream(outFollowing);
		closeDataOutputStream(outFollowers);
		closeDataOutputStream(outTweetsIndexes);
		closeDataOutputStream(outTweets);
        closeScanner(inTweets);
        closeScanner(inFollowing);
        
        File file = new File(FOLLOWERS_BLOCK + "1" + FILE_SUFFIX);
        file.delete();
	}
		
	/** Reads the sorted file of followers .*/
	private void readFollowersSorted(NodeData current,DataInputStream inFollowers) 
	{
		int id = current.getId();
		try 
		{
			if (_prevIdForFollowers == -1)
			{
				_prevIdForFollowers = inFollowers.readInt();
			}
			if (_prevIdForFollowers == id)
			{
				current.addFollower(inFollowers.readInt());
				while( (_prevIdForFollowers = inFollowers.readInt()) == id)
				{
					current.addFollower(inFollowers.readInt());
				}
			}
		}
		catch (EOFException e) {}
		catch (IOException e) {}
	}
	
	/** Read edges.txt storing the following. */
	private void readFollowingSorted(NodeData current,Scanner s)
	{
		int id = current.getId();
		if ( (s.hasNext()) && (_prevIdForFollowing == -1))
		{
			_prevIdForFollowing = Integer.parseInt(s.next());
		}
		if ( (s.hasNext()) && (_prevIdForFollowing == id))
		{
			current.addFollowing(Integer.parseInt(s.next()));
			while( (s.hasNext()) && ((_prevIdForFollowing = Integer.parseInt(s.next())) == id) )
			{
				current.addFollowing(Integer.parseInt(s.next()));
			}
		}
	}
	
	/** Performs external merge sort.*/
	private void performExtMergeSort(int numOfRuns, String firstPartOfName,String fileSuffix, byte type,int blockSize)
	{
        ExtMergeSort ems = new ExtMergeSort(type);
        ems.mergeSortRuns(numOfRuns,firstPartOfName,fileSuffix,blockSize);
	}
	
	/** Reads edges.txt and saves for each nodeId its following and followers. */
	private void readEdgesTxt() 
	{
		Scanner s = openScanner(EDGES_TXT);
		int id,following, followersRuns = 1;//followersRuns2 = 1;
		BlockOfPairs followersBlock = new BlockOfPairs(FOLLOWERS_BLOCK + followersRuns + FILE_SUFFIX);
		boolean finish2;
        while (s.hasNext())
        {
        	id = Integer.parseInt(s.next());
        	following = Integer.parseInt(s.next());
        	finish2 = followersBlock.add(false,true,BUFFER_BLOCK,following, id);
        	if (finish2 == true)
        	{
        		followersBlock.setFileName(FOLLOWERS_BLOCK + (followersRuns) + FILE_SUFFIX);
        		followersBlock = new BlockOfPairs(FOLLOWERS_BLOCK + (++followersRuns) + FILE_SUFFIX);
        	}
        }
        // Writes the current block in case it does not full.
        followersBlock.writeBlock(false);
    	closeScanner(s);
    	if (followersRuns > 1)
    	{
    		performExtMergeSort(followersRuns,FOLLOWERS_BLOCK,FILE_SUFFIX,PAIR,BUFFER_BLOCK);
    	}
    }
	
	/** Increases the frequency of a word in the dictionary. */
	private void increaseDictFreq(int tweetNum)
	{
		int nodeId = Integer.parseInt(_wordsOfTweet[0]);
		for (int i = 1; i < _wordsOfTweet.length; i++)
		{
			_dictData.get(Integer.parseInt(_wordsOfTweet[i])).increaseFreq(nodeId,tweetNum);
		}
	}
	
	/** Adds the the serial number of the tweet, in relation to NodeId,
	 *  as listed in a tweets.txt
	 */
	private void addTweetNum(int nodeId, int tweetNum) 
	{
		int wordNum;
		for (int i = 1; i < _wordsOfTweet.length; i++)
		{
			wordNum = Integer.parseInt(_wordsOfTweet[i]);
			
	        if (_dictNodesBlock.add(false,true,BUFFER_BLOCK2,wordNum, nodeId,tweetNum) == true)
			{
	        	_dictNodesBlock.setFileName(DICT_NODES_BLOCK + (_dictNodesRuns) + FILE_SUFFIX);
	        	_dictNodesBlock = new BlockOfThree(DICT_NODES_BLOCK + (++_dictNodesRuns) + FILE_SUFFIX);
	        	 System.gc();
		    }
		}
	}
	
	/** Reads tweets.txt and process the data. */
	private void readTweetsTxt(int nodeId, Scanner inTweets,DataOutputStream outTweetsIndexes,DataOutputStream outTweets,NodeData current) 
	{
		int prevTweetIndexOffset = nodeId,currentTweetIndexOffset = _currentTweetsBytes, tweetNum = 0;
		if ( (_wordsOfTweet != null) && (_wordsOfTweet.length > 0) && (Integer.parseInt(_wordsOfTweet[0]) == nodeId))
		{
			nodeId = Integer.parseInt(_wordsOfTweet[0]);
			current.addTweet(_wordsOfTweet);
			current.writeTweet(outTweets);
        	increaseDictFreq(tweetNum);
        	if (tweetNum == 0)
        	{
        		current.addTweetsPointer(prevTweetIndexOffset,currentTweetIndexOffset,true);
        	}
        	else
        	{
        		current.addTweetsPointer(prevTweetIndexOffset,currentTweetIndexOffset,false);
        	}
        	addTweetNum(nodeId,tweetNum);
        	prevTweetIndexOffset = currentTweetIndexOffset;
        	currentTweetIndexOffset += current.getSize(TWEETS);
        	tweetNum++;
		}
        if ( (_wordsOfTweet == null) || (tweetNum == 1) )
        {
        	while (inTweets.hasNextLine()) 
        	{
        		_wordsOfTweet = inTweets.nextLine().split(WORD_SEPERATOR);
        		if (Integer.parseInt(_wordsOfTweet[0]) != nodeId)
        		{
        			break;
        		}	
    			nodeId = Integer.parseInt(_wordsOfTweet[0]);
    			current.addTweet(_wordsOfTweet);
    			current.writeTweet(outTweets);
        		increaseDictFreq(tweetNum);
        		if (tweetNum == 0)
        		{
            		current.addTweetsPointer(prevTweetIndexOffset,currentTweetIndexOffset,true);
        		}
        		else
        		{
        			current.addTweetsPointer(prevTweetIndexOffset,currentTweetIndexOffset,false);
        		}
        		addTweetNum(nodeId,tweetNum);
        		prevTweetIndexOffset = currentTweetIndexOffset;
        		currentTweetIndexOffset += current.getSize(TWEETS);
        		tweetNum++;
        	}
        }
        if (tweetNum > 0) // if nodeId has at least one tweet.
        {
        	current.addTweetsPointer(prevTweetIndexOffset,currentTweetIndexOffset,false);
        	current.writeTweetsPointers(outTweetsIndexes); //writes the encoded tweet.
        	_currentTweetsIndexesBytes += current.getSize(TWEETS_INDEX);
        	_currentTweetsBytes = currentTweetIndexOffset;
        }
	}
	
	/** Reads names.txt and process the data. */
	private void readNamesTxt()
	{
		Scanner s = openScanner(NAMES_TXT);
		NameData current = null;
		String name;
		int nameNum = 0;
		while (s.hasNext()) 
		{
            name = s.next();
            current = new NameData(name,nameNum);
            _namesData.add(current);
            nameNum++;
        }
		closeScanner(s);
	}
	
	/** Reads the sorted file of nodes of names and stores names (names numbers) is current NameData. */
	private void readNodesOfNamesSorted(NameData current,DataInputStream in) 
	{
		int id = current.getId();
		try 
		{
			if (_prevIdForNames == -1)
			{
				_prevIdForNames = in.readInt();
			}
			if (_prevIdForNames == id)
			{
				current.addNode(in.readInt());
				while( (_prevIdForNames = in.readInt()) == id)
				{
					current.addNode(in.readInt());
				}
			}
		}
		catch (EOFException e) {}
		catch (IOException e) {}
	}
	
	/** Writes data for the indexes associated with the names.*/
	private void writeNamesIndex()
	{
		int prevIdsBytes = 0;
		DataOutputStream outNames = openDataOutputStream(NAMES_INDEX),
		outNamesString = openDataOutputStream(NAMES_STRING),
		outNodesListIndex = openDataOutputStream(NODES_OF_NAME_POINTERS),
		outNodes = openDataOutputStream(NODES_OF_NAME);
		_fCoding.clearAll();
		
        _nodesNamesBlock.writeBlock(true); // Writes the last block in case it does not full.
        if (_nodesNamesRuns > 1)
        {
        	performExtMergeSort(_nodesNamesRuns,NODES_OF_NAMES_BLOCK,FILE_SUFFIX,PAIR,BUFFER_BLOCK);
        }
        
		DataInputStream in = openDataInputStream(NODES_OF_NAMES_BLOCK + "1" + FILE_SUFFIX);
		for (int i = 0; i < _namesData.size(); i++)
		{
			_fCoding.encodeWord(_namesData.get(i).getName());
			readNodesOfNamesSorted(_namesData.get(i),in);
			_namesData.get(i).write(outNames,outNamesString,outNodesListIndex,outNodes,
									(byte)_fCoding.getPrefixLength(),(byte)_fCoding.getPostfixLength(),
									_fCoding.getPostfixBytes(),prevIdsBytes);
			prevIdsBytes += _namesData.get(i).getSize();
			_namesData.get(i).clearAll();
		}
		try 
		{
			outNames.writeInt(_namesData.size());
		} 
		catch (IOException e) {}
		closeDataOutputStream(outNames);
		closeDataOutputStream(outNamesString);
		closeDataOutputStream(outNodesListIndex);
		closeDataOutputStream(outNodes);
		closeDataInputStream(in);
        File file = new File(NODES_OF_NAMES_BLOCK + "1" + FILE_SUFFIX);
        file.delete();
	}
	
	/** Reads dict.txt and process the data. */
	private void readDictTxt()
	{
		Scanner s = openScanner(DICT_TXT);
		DictData current = null;
		String word;
		int wordNum = 0;
		while (s.hasNext()) 
		{
            word = s.next();
            current = new DictData(word,wordNum);
            _dictData.add(current);
            wordNum++;
        }
		closeScanner(s);
	}
	
	/** Reads the sorted file of tweets numbers of each nodes which has at least one tweet with the current word. */
	private void readDictNodesSorted(DictData current,DataInputStream in) 
	{
		int wordId = current.getId(), nodeId;
		try 
		{
			if (_prevIdForWords == -1)
			{
				_prevIdForWords = in.readInt();
			}
			if (_prevIdForWords == wordId)
			{
				current.addNode( (nodeId = in.readInt()) );
				current.addToNodeTweetsList(nodeId,in.readInt());
				while( (_prevIdForWords = in.readInt()) == wordId)
				{
					current.addNode( (nodeId = in.readInt()) );
					current.addToNodeTweetsList(nodeId,in.readInt());
				}
			}
		}
		catch (EOFException e) {}
		catch (IOException e) {}
	}
	
	/** Writes data for the indexes associated with the dictionary.*/
	private void writeDictIndex()
	{
		int prevNodesPointersBytes = 0, prevTweetsNumbersOfNodesBytes = 0; 
		DataOutputStream outDict = openDataOutputStream(DICT_INDEX),
		outDictString = openDataOutputStream(DICT_STRING),
		outNodesPointers = openDataOutputStream(DICT_NODES_POINTERS_TWEETS),
		outTweetsNumbersOfNodes = openDataOutputStream(DICT_TWEETS_NUMBERS);
		_fCoding.clearAll();
		
        _dictNodesBlock.writeBlock(true); // Writes the last block in case it does not full.
        if (_dictNodesRuns > 1)
        {
        	performExtMergeSort(_dictNodesRuns,DICT_NODES_BLOCK,FILE_SUFFIX,THREE,BUFFER_BLOCK2);
        }
        
		DataInputStream in = openDataInputStream(DICT_NODES_BLOCK + "1" + FILE_SUFFIX);
		for (int i = 0; i < _dictData.size(); i++)
		{
			_fCoding.encodeWord(_dictData.get(i).getWord());
			readDictNodesSorted(_dictData.get(i),in);
			_dictData.get(i).write(outDict,outDictString,outNodesPointers,outTweetsNumbersOfNodes,
									(byte)_fCoding.getPrefixLength(),(byte)_fCoding.getPostfixLength(),
									_fCoding.getPostfixBytes(),prevNodesPointersBytes,
									prevTweetsNumbersOfNodesBytes);
			prevNodesPointersBytes += _dictData.get(i).getBytesNumOfNodes();
			prevTweetsNumbersOfNodesBytes += _dictData.get(i).getBytesOfTweetsNumbers();
			_dictData.get(i).clearAll();
		}
		closeDataOutputStream(outDict);
		closeDataOutputStream(outDictString);
		closeDataOutputStream(outNodesPointers);
		closeDataOutputStream(outTweetsNumbersOfNodes);
		closeDataInputStream(in);
        File file = new File(DICT_NODES_BLOCK + "1" + FILE_SUFFIX);
        file.delete();
	}
}