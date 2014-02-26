/** tamirtf77
 *  
 *  This class implements the index reader.
 */

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

public class IndexReader 
{
	
	private static final int CHAR_SIZE = 16;
	private static final int NOT_FOUND = -1;
	private static final String EMPTY = "";
	private static final String WORD_SEPERATOR = " ";
	private static final String FOR_READING = "r";
	private static final int BINARY_RADIX = 2;
	
	private static final int NUM_OF_NODES_BYTES = 4;
	
	private static final int OUT_DEGREE_OFFSET = 2;
	private static final int IN_DEGREE_OFFSET = 6;
	private static final int FOLLOWING_OFFSET = 10;
	private static final int FOLLOWERS_OFFSET = 14;
	private static final int TWEETS_OFFEST = 18;
	private static final int NODE_JUMP_BYTES = 22;
	
	private static final int NAMES_JUMP_BYTES = 2;
	private static final int NUM_OF_NAMES_JUMP_BYTES = 4;
	private static final int NUM_OF_NAMES_BYTES = 4;

	private static final int FREQ_OFFSET = 2;
	private static final int DICT_POINTER_POS_OFFSET = 6;
	private static final int DICT_JUMP_BYTES = 10;
	
	private ArrayList<String> _names = new ArrayList<String>();
	private ArrayList<String> _words = new ArrayList<String>();
	private int _numOfNodes,_numOfNames;
	private boolean _namesDecoded = false, _wordsDecoded = false,
			        _readNumOfNodes = false, _readNumOfNames = false;
	
/**
* Returns the name of a node
* Returns null if the node does not exist in the network
*/
	public String getName(int nodeId) 
	{
		File file = new File(IndexWriter.NODES_INDEX),
			 file2 = new File(IndexWriter.NAMES_INDEX);
		RandomAccessFile inNodesIndex = open(file), inNamesIndex = open(file2);
		//ArrayList<String> names = new ArrayList<String>();
		String name = null;
		int nameNum;
		long pos = nodeId*NODE_JUMP_BYTES;
		try 
		{
			if (_readNumOfNodes == false)
			{
				inNodesIndex.seek(file.length() - NUM_OF_NODES_BYTES);
				_numOfNodes = inNodesIndex.readInt();
				_readNumOfNodes = true;
			}
			if (nodeId < _numOfNodes)
			{
				inNodesIndex.seek(pos);
				name = Integer.toBinaryString(inNodesIndex.readChar());
				if (name.length() >= CHAR_SIZE - 1) // the privacy is saved in the second bit from left.
				{
					name = name.substring(1,name.length());
				}
				nameNum = Integer.parseInt(name,BINARY_RADIX);
				if (_namesDecoded == false)
				{
					decodeNames(inNamesIndex,_names,file2.length()- NUM_OF_NODES_BYTES,IndexWriter.NAMES_STRING,NAMES_JUMP_BYTES);
					_namesDecoded = true;
				}
				name = _names.get(nameNum);
			}
		}
		catch (IOException e){}
		close(inNodesIndex); close(inNamesIndex);
		return name;
	}
	
/**
* Returns the privacy status of a node, i.e., true if the node
* has privacy status public
* Returns false if the node does not exist in the network (or if
* its tweets are private)
*/
	public boolean isPublic(int nodeId)
	{
		File file = new File(IndexWriter.NODES_INDEX);
		RandomAccessFile in = open(file);
		String s = null;
		boolean privacy = false;
		long pos = nodeId*NODE_JUMP_BYTES;
		try 
		{
			if (_readNumOfNodes == false)
			{
				in.seek(file.length() - NUM_OF_NODES_BYTES);
				_numOfNodes = in.readInt();
				_readNumOfNodes = true;
			}
			if (nodeId < _numOfNodes)
			{
				in.seek(pos);
				s = Integer.toBinaryString(in.readChar());
				// The privacy is saved in the second bit from left.
				// if the node has privacy status public.
				if (s.length() >= CHAR_SIZE - 1)
				{
					privacy = true;
				}
			}
		}
		catch (IOException e){}
		close(in);
		return privacy;
	}
	
	/** Gets the following or followers of a nodeId according to the fileName and pos.
	 * 
	 * @param nodeId - the id of the node.
	 * @param fileName - following.txt or followers.txt
	 * @param pos - the position in the file nodes_index.txt in order to access
	 * the exact location in the file of the pointer to the beginning of the followers/following list.
	 * @return the following/followers of an nodeId.
	 */
	private Enumeration<Integer> getFollows(int nodeId,String fileName,long pos)
	{
		File file = new File(IndexWriter.NODES_INDEX);
		File file2 = new File(fileName);
		RandomAccessFile in = open(file), inFollow = open(file2);
		long pointer1,pointer2;
		GroupVarintEncoding g = new GroupVarintEncoding();
		try 
		{
			if (_readNumOfNodes == false)
			{
				in.seek(file.length() - NUM_OF_NODES_BYTES);
				_numOfNodes = in.readInt();
				_readNumOfNodes = true;
			}
			if (nodeId < _numOfNodes)
			{
				in.seek(pos);
				pointer1 = in.readInt();
				inFollow.seek(pointer1);
				if ( (nodeId+1) == _numOfNodes)
				{
					pointer2 = file2.length();
				}
				else
				{
					in.seek(pos + NODE_JUMP_BYTES);// gets to the pointer of the next node id.
					pointer2 = in.readInt();
				}
				for (long i = 0; i < pointer2 - pointer1; i++)
				{
					g.addDecoded(inFollow.readByte());
				}
				g.decodeGroups();
			}
			else
			{
				close(in); close(inFollow);
				return null;
			}
		}
		catch (IOException e){}
		close(in); close(inFollow);
		return decodeDiffs(nodeId,g);
	}
	
	/** Opens a RandomAccessFile to a file which is given as a parameter. */
	private RandomAccessFile open(File f)
	{
		RandomAccessFile o = null;
		try 
		{
			o = new RandomAccessFile(f,FOR_READING);
		}
		catch (FileNotFoundException e) {}
		return o;
	}
	
	/** Closes a RandomAccessFile. */
	private void close(RandomAccessFile o)
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
		DataInputStream o = null;
		try 
		{
			o = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
		} 
		catch (FileNotFoundException e) {}
		return o;
	}
	
	/** Closes a DataOutputStream. */
	private void closeDataInputStream(DataInputStream o)
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
	
	/** Decoded the differences of a decoded list using gap decompression. */
	private Enumeration<Integer> decodeDiffs(int nodeId, GroupVarintEncoding g)
	{
		Vector<Integer> follows = new Vector<Integer>(g.decodedSize());
		int current,prev,i = 0;
		if (g.decodedSize() > 0)
		{
			prev = g.getDecoded(i);
			if ((prev % 2) == 0)
			{
				prev = prev / 2;
				prev = prev + nodeId;
			}
			else
			{
				prev++;
				prev = prev / 2;
				prev = nodeId - prev;
			}
			follows.add(prev);
			for(i = 1; i < g.decodedSize();i++)
			{
				current = g.getDecoded(i) + prev + 1;
				follows.add(current);
				prev = current;
			}
		}
		return follows.elements();
	}
	
	/** Decoded the differences of a words' numbers list of a tweet
	 *  with the exactly gap compression - the words' numbers could not be in ascending order,
	 *  so for each gap we use like in the first entry of gap compression.
	 */
	private Enumeration<Integer> decodeTweetsDiffs(int nodeId, GroupVarintEncoding g)
	{
		Vector<Integer> follows = new Vector<Integer>(g.decodedSize());
		int current = nodeId ,prev;
		if (g.decodedSize() > 0)
		{
			for(int i = 0; i < g.decodedSize();i++)
			{
				prev = g.getDecoded(i);
				if ((prev % 2) == 0)
				{
					prev = prev / 2;
					prev = prev + current;
				}
				else
				{
					prev++;
					prev = prev / 2;
					prev = current - prev;
				}
				follows.add(prev);
				current = prev;
			}
		}
		return follows.elements();
	}
	
/**
* Returns the ids of friends of a node
* Given nodeId i, return all j such that (i,j) is in the network,
* sorted by ascending order of j
* Returns null if the node does not exist in the network
*/
	public Enumeration<Integer> getFollowing(int nodeId) 
	{
		long pos = nodeId*NODE_JUMP_BYTES + FOLLOWING_OFFSET;
		return getFollows(nodeId,IndexWriter.FOLLOWING,pos);
	}

/**
* Returns the ids of nodes who consider the input node their friend
* Given nodeId i, return all j such that (j,i) is in the network,
* sorted by ascending order of j
* Returns null if the node does not exist in the network
*/
	public Enumeration<Integer> getFollowers(int nodeId) 
	{
		long pos = nodeId*NODE_JUMP_BYTES + FOLLOWERS_OFFSET;
		return getFollows(nodeId,IndexWriter.FOLLOWERS,pos);
	}

/**
* Returns the tweets of a given node, sorted by the order that they
* appeared in the file tweets.txt
* Word numbers in tweets should be translated into the corresponding words
* Returns null if the node does not exist in the network
*/
	public Enumeration<String> getTweets(int nodeId) 
	{
		long pos = nodeId*NODE_JUMP_BYTES + TWEETS_OFFEST;
		Enumeration<Integer> tweetsNums = getFollows(nodeId,IndexWriter.TWEETS_INDEX,pos);
		if (tweetsNums == null)
		{
			return null;
		}
		File file = new File(IndexWriter.TWEETS),
			 file2 = new File(IndexWriter.DICT_INDEX);
		RandomAccessFile inTweets = open(file),inDict = open(file2);
		//ArrayList<String> words = new ArrayList<String>();
		Vector<String> tweets = new Vector<String>();
		int p1 = 0, p2 = 0;
		GroupVarintEncoding g = new GroupVarintEncoding();
		String tweet = EMPTY;
		if (_wordsDecoded == false)
		{
			decodeNames(inDict,_words,file2.length(),IndexWriter.DICT_STRING,DICT_JUMP_BYTES);
			_wordsDecoded = true;
		}
		try
		{
			if (tweetsNums.hasMoreElements())
			{
				p1 = tweetsNums.nextElement();
				inTweets.seek(p1);
			}
			while (tweetsNums.hasMoreElements())
			{
				p2 = tweetsNums.nextElement();
				for (int i = 0; i < p2 - p1; i++)
				{
					g.addDecoded(inTweets.readByte());
				}
				g.decodeGroups();
				Enumeration<Integer> wordsNums = decodeTweetsDiffs(nodeId,g);
				while (wordsNums.hasMoreElements())
				{
					tweet += _words.get( wordsNums.nextElement()) + WORD_SEPERATOR;
				}
				tweet = tweet.substring(0,tweet.length()-1); // removes the last space.
				tweets.add(tweet);
				g.clearAll();
				tweet = EMPTY;
				p1 = p2;
				inTweets.seek(p1);
			}
		}
		catch (IOException e){}
		close(inTweets); close(inDict);
		return tweets.elements();
	}
	
	/** Gets a degree of a nodeId.
	 * 
	 * @param nodeId the node id to get its degree.
	 * @param addOffset the offset in order to get the degree. 
	 * if it's 2 then it returns the out degree, if it's 3 then it
	 * returns the in degree.
	 * @return the out / in degree according to addOffset.
	 */
	private int getDegree(int nodeId, int addOffset)
	{
		File file = new File(IndexWriter.NODES_INDEX);
		RandomAccessFile in = open(file);
		int degree = NOT_FOUND; //numOfNodes
		long pos = nodeId*NODE_JUMP_BYTES + addOffset;
		try 
		{
			if (_readNumOfNodes == false)
			{
				in.seek(file.length() - NUM_OF_NODES_BYTES);
				_numOfNodes = in.readInt();
				_readNumOfNodes = true;
			}
			if (nodeId < _numOfNodes)
			{
				in.seek(pos);
				degree = in.readInt();
			}
		}
		catch (IOException e){}
		finally
		{
			close(in);
		}
		return degree;
	}

/**
* Returns the number of nodes that would be returned by getFollowing
* Returns -1 if the node does not exist in the network
*/
	public int getOutDegree(int nodeId) 
	{
		return getDegree(nodeId,OUT_DEGREE_OFFSET);
	}

/**
* Returns the number of nodes that would be returned by getFollowers
* Returns -1 if the node does not exist in the network
*/
	public int getInDegree(int nodeId)
	{
		return getDegree(nodeId,IN_DEGREE_OFFSET);
	}
	
	/** Decodes words / names which are compressed using front coding. */
	private void decodeNames(RandomAccessFile in, ArrayList<String> names, long fileLength,
							 String stringFile, int jumpBytes) 
	{
		DataInputStream inString = openDataInputStream(stringFile);
		long pos1 = 0;
		int prefixLength,postfixLength;
		String current = EMPTY, prefix = EMPTY, prev = EMPTY;
		while (pos1 < fileLength)
		{
			try 
			{
				in.seek(pos1);
				prefixLength = in.readByte();
				in.seek(pos1+1);
				postfixLength = in.readByte();
				pos1 += jumpBytes;
				for (int i = 1; i <= postfixLength; i++) //creating postfix.
				{
					char[] ch = Character.toChars(inString.readByte());
					current = current.concat(String.copyValueOf(ch));
				}
				prefix = prefix.concat(prev.substring(0, prefixLength));
				current = prefix.concat(current);
				names.add(current);
				prev = current;
				current = EMPTY;
				prefix = EMPTY;
			}
			catch (IOException e) {}
		}
		closeDataInputStream(inString);
	}
	
	/** Perform a binary search */
    private int binarySearch(ArrayList<String> search, String name) 
    {
    	int start, end, mid, val;
        start = 0;
        end = search.size() - 1;
        while (start <= end) 
        {
        	mid = (start + end) / 2;
        	val = search.get(mid).compareTo(name);
            if (val == 0) 
            {
            	return mid;
            } 
            else if (val < 0) 
            {
            	start = mid + 1;
            } 
            else 
            {
            	end = mid - 1;
            }
        }
        return NOT_FOUND;
}

/**
* Returns the ids of nodes with a given name, sorted by
* ascending node id
* This is case sensitive
* Returns null if the name does not exist in the network
*/
	public Enumeration<Integer> getNodesByName(String name) 
	{
		//ArrayList<String> names = new ArrayList<String>();
		File file3 = new File(IndexWriter.NAMES_INDEX);
		RandomAccessFile inNamesIndex = open(file3);
		if (_namesDecoded == false)
		{
			decodeNames(inNamesIndex,_names,file3.length()-NUM_OF_NAMES_BYTES,IndexWriter.NAMES_STRING,NAMES_JUMP_BYTES);
			_namesDecoded = true;
		}
		int index = binarySearch(_names,name);
		if (index == NOT_FOUND)
		{
			return null;
		}
		File file = new File(IndexWriter.NODES_OF_NAME_POINTERS);
		File file2 = new File(IndexWriter.NODES_OF_NAME);
		RandomAccessFile inPointers = open(file),inNodes = open(file2);
		long pointer1 = 0, pointer2 = 0;
		GroupVarintEncoding g = new GroupVarintEncoding();
		try
		{
			if (_readNumOfNames == false)
			{
				inNamesIndex.seek(file3.length() - NUM_OF_NAMES_BYTES);
				_numOfNames = inNamesIndex.readInt();
				_readNumOfNames = true;
			}
			if (index < _numOfNames)
			{
				inPointers.seek(index*NUM_OF_NAMES_JUMP_BYTES);
				pointer1 = inPointers.readInt();
				inNodes.seek(pointer1);
				if ( (index+1) == _numOfNames)
				{
					pointer2 = file2.length();
				}		
				else
				{
					pointer2 = inPointers.readInt();
				}
			}
			for (long i = 0; i < pointer2 - pointer1; i++)
			{
				g.addDecoded(inNodes.readByte());
			}
			g.decodeGroups();
		}
		catch(IOException e){}
		close(inNodes);
		close(inPointers);
		return decodeDiffs(index,g);
	}
	

/**
* Given a nodeId i and a word s, return pairs of
* node ids and tweets (j,t) such that j tweeted t, t contains
* the word s, and this tweet is visible to node i.
* Word numbers in tweets should be translated into the corresponding words
* This is case sensitive
* Return value is sorted by node id (from smallest to largest), and within this
* order, tweets should be returned in the order they appeared in tweets.txt
* Returns an empty enumeration if the word does not appear in any tweet
* visible to the user
*/
	public Enumeration<Map.Entry<Integer,String>> getTweetsByWord(String word,int nodeId)
	{
		File file = new File(IndexWriter.DICT_INDEX),
			 file2 = new File(IndexWriter.DICT_NODES_POINTERS_TWEETS),
			 file3 = new File(IndexWriter.DICT_TWEETS_NUMBERS);
		//ArrayList<String> words = new ArrayList<String>();
		RandomAccessFile in = open(file),inNodesPointersTweets = open(file2),
						 inTweetsNumbers = open(file3);
		Vector<Map.Entry<Integer,String>> tweetsNodes = new Vector<Map.Entry<Integer,String>>();
		if (_wordsDecoded == false)
		{
			decodeNames(in,_words,file.length(),IndexWriter.DICT_STRING,DICT_JUMP_BYTES);
			_wordsDecoded = true;
		}
		int dictNum = binarySearch(_words,word);
		if (dictNum ==  NOT_FOUND)
		{
			//System.out.println("NOT_FOUND");
			close(in);
			return tweetsNodes.elements();
		}
		long currentDictPointerPos = dictNum * DICT_JUMP_BYTES + DICT_POINTER_POS_OFFSET,
		     nextDictPointerPos = (dictNum + 1) * DICT_JUMP_BYTES + DICT_POINTER_POS_OFFSET;
		long currentPointerVal, nextPointerVal,currentNodesPointersTweetsVal,nextNodesPointersTweetsVal;
		GroupVarintEncoding g = new GroupVarintEncoding();
		try 
		{
			in.seek(currentDictPointerPos);
			currentPointerVal = in.readInt();
			in.seek(nextDictPointerPos);
			nextPointerVal = in.readInt();	
			if (currentPointerVal == nextPointerVal) // it means the word does not appear in any tweet.
			{
				//System.out.println("NOT APPEAR");
				close(in);
                return tweetsNodes.elements();
			}
			
			inNodesPointersTweets.seek(currentPointerVal);
			currentNodesPointersTweetsVal = inNodesPointersTweets.readInt();
			nextNodesPointersTweetsVal = inNodesPointersTweets.readInt();
			inTweetsNumbers.seek(currentNodesPointersTweetsVal);
			//reads the nodes IDs which has at least one tweet with the word.
			for (long i = 0; i < nextNodesPointersTweetsVal - currentNodesPointersTweetsVal; i++)
			{
				g.addDecoded(inTweetsNumbers.readByte());
			}
			g.decodeGroups();
			Enumeration<Integer> nodesIds =  decodeDiffs(dictNum,g);
			Enumeration<Integer> followingTemp = getFollowing(nodeId);
			Vector<Integer> following = new Vector<Integer>();
			while ((followingTemp != null) && (followingTemp.hasMoreElements()) )
			{
				following.add(followingTemp.nextElement());
			}
			int nodeContainWord,nodeFollowing;
			boolean found = false;
			while ( (nodesIds != null) && (nodesIds.hasMoreElements()) )
			{
				nodeContainWord = nodesIds.nextElement();
				currentNodesPointersTweetsVal = nextNodesPointersTweetsVal;
				nextNodesPointersTweetsVal = inNodesPointersTweets.readInt();
				inTweetsNumbers.seek(currentNodesPointersTweetsVal);
				for (int k = 0; k < following.size() && (found == false); k++)
				{
					nodeFollowing = following.get(k);
					if ( (nodeContainWord == nodeFollowing) || (isPublic(nodeContainWord) == true) )
					{
						found = true;
						g.clearAll();
						for (long i = 0; i < nextNodesPointersTweetsVal - currentNodesPointersTweetsVal; i++)
						{
							g.addDecoded(inTweetsNumbers.readByte());
						}
						g.decodeGroups();
						Enumeration<Integer> tweetsIds =  decodeDiffs(dictNum,g);
						while (tweetsIds.hasMoreElements())
						{
							int t = tweetsIds.nextElement();
							getTweetContainsWord(nodeContainWord,t,_words,tweetsNodes);
						}
					}
				}
				found = false;
			}
		}
		catch (IOException e) {}
		close(in); close(inNodesPointersTweets); close(inTweetsNumbers);
		return tweetsNodes.elements();
	}

	/** Gets the tweet's string of a tweet which contains a specific word according to nodeId and tweetId. */
	private void getTweetContainsWord(int nodeId,int tweetId,ArrayList<String> words, Vector<Map.Entry<Integer,String>> tweetsNodes)
	{
		long pos = nodeId*NODE_JUMP_BYTES + TWEETS_OFFEST;
		Enumeration<Integer> tweetsNums = getFollows(nodeId,IndexWriter.TWEETS_INDEX,pos);
		File file5 = new File(IndexWriter.TWEETS);
		RandomAccessFile inTweets = open(file5);
		int p1 = 0, p2 = 0, j = 0;
		GroupVarintEncoding g2 = new GroupVarintEncoding();
		String tweet = EMPTY;
		try
		{
			if (tweetsNums.hasMoreElements())
			{
				p1 = tweetsNums.nextElement();
				inTweets.seek(p1);
			}
			while (tweetsNums.hasMoreElements())
			{
				p2 = tweetsNums.nextElement();
				if (j == tweetId)
				{
					for (int i = 0; i < p2 - p1; i++)
					{
						g2.addDecoded(inTweets.readByte());
					}
					g2.decodeGroups();
					Enumeration<Integer> emm = decodeTweetsDiffs(nodeId,g2);
					while (emm.hasMoreElements())
					{
						tweet += words.get(emm.nextElement()) + WORD_SEPERATOR;
					}
					tweet = tweet.substring(0,tweet.length()-1); // removes the last space.
					Entry<Integer, String> entry = new AbstractMap.SimpleEntry<Integer,String>(nodeId,tweet);
					tweetsNodes.add(entry);
					g2.clearAll();
					tweet = EMPTY;
				}
				p1 = p2;
				inTweets.seek(p1);
				j++;
			}
			close(inTweets);
		}
		catch (IOException e){}
	}

/**
* Returns the frequency of the given word, i.e., the number of tweets
* containing this word. If the word does not exist, return 0.
*/
	public int getWordFrequency(String word) 
	{
		File file = new File(IndexWriter.DICT_INDEX);
		//ArrayList<String> words = new ArrayList<String>();
		RandomAccessFile in = open(file);
		if (_wordsDecoded == false)
		{
			decodeNames(in,_words,file.length(),IndexWriter.DICT_STRING,DICT_JUMP_BYTES);
			_wordsDecoded = true;
		}
		int index = binarySearch(_words,word), freq = 0;
		if (index != NOT_FOUND)
		{
			long freqPos = index * DICT_JUMP_BYTES + FREQ_OFFSET;
			try 
			{
				in.seek(freqPos);
				freq = in.readInt();
			} 
			catch (IOException e) {}
		}
		return freq;
	}
}