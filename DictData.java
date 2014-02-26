/** tamirtf77
 *  
 *  This class stores data about a word in the dictionary. 
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class DictData 
{
	private static final int POINTER_SIZE = 4;
	
	private String _word;
	private int _dictNum,_freq,_prevNode,_currentNodeId,_currentTweetNum;
	private GroupVarintEncoding _gNodesIds;
	private boolean _firstNode;
	
	// for each node contains the word, we should store the tweets numbers
	// of that node, as they appear in tweets.txt
	private Map<Integer,TweetsNumbers>_tweetsNumbersOfNodes;

	public DictData(String word,int dictNum)
	{
		_word = word;
		_dictNum = dictNum;
		_gNodesIds = new GroupVarintEncoding();
		_tweetsNumbersOfNodes = new TreeMap<Integer, TweetsNumbers>();
		_currentNodeId = -1;
		_currentTweetNum = -1;
		_freq = 0;
		_firstNode = true;
	}
	
	/** adds a node id which contains at least one tweet with the word. */
	public void addNode(int currentNode)
	{
		if (!_tweetsNumbersOfNodes.containsKey(currentNode))
		{
			if (_firstNode == true)
			{
				_prevNode = _dictNum;
				_gNodesIds.clearAll();
				if ((currentNode - _prevNode) >= 0)
				{
					_gNodesIds.addToEncode(2 * (currentNode - _prevNode));
				}
			    else
			    {
			    	_gNodesIds.addToEncode(2 * (-1) * (currentNode - _prevNode) - 1); // s1 - x
				}
				_firstNode = false;
			}
			else
			{
				_gNodesIds.addToEncode(currentNode - _prevNode - 1);
			}
			_prevNode = currentNode;
			TweetsNumbers t = new TweetsNumbers();
			_tweetsNumbersOfNodes.put(currentNode,t);
		}
	}
	
	/** adds a tweet's number which containing the word 
	 *  to the list of tweets' numbers of an node id . 
	 */
	public void addToNodeTweetsList(int nodeId,int tweetNum)
	{
		_tweetsNumbersOfNodes.get(nodeId).addTweetNum(tweetNum);
	}
	
	public void write(DataOutputStream outDict,DataOutputStream outDictString,
				      DataOutputStream outNodesPointers,DataOutputStream outTweetsNumbersOfNodes,
				      byte prefixLength,byte postfixLength,
				      ArrayList<Byte> postfix,int prevNodesPointersBytes,int prevTweetsNumbersOfNodes) 
	{
		try 
		{
			outDict.writeByte(prefixLength);
			outDict.writeByte(postfixLength);
			outDict.writeInt(_freq);
			outDict.writeInt(prevNodesPointersBytes);
			for (int i = 0; i < postfix.size(); i++)
			{
				outDictString.writeByte(postfix.get(i));
			}
			_gNodesIds.encodeGroups();
			for (int i = 0; i < _gNodesIds.size(); i++)
			{
				outTweetsNumbersOfNodes.writeByte(_gNodesIds.getEncodedByte(i));
			}
			int currentTweetsNumbersOfNodes = prevTweetsNumbersOfNodes;
			currentTweetsNumbersOfNodes += _gNodesIds.size();
			if (currentTweetsNumbersOfNodes != prevTweetsNumbersOfNodes)
			{
				outNodesPointers.writeInt(prevTweetsNumbersOfNodes);
			}
			for (TweetsNumbers value : _tweetsNumbersOfNodes.values())
			{
				outNodesPointers.writeInt(currentTweetsNumbersOfNodes);
				value.write(outTweetsNumbersOfNodes);
				currentTweetsNumbersOfNodes += value.getSize();
			}
		} 
		catch (IOException e) {}
	}
	
	public int getId()
	{
		return _dictNum;
	}
	
	/** Gets the word. */
	public String getWord()
	{
		return _word;
	}
	
	/** Gets the number of nodes, 
	 *  which containing at least one tweet of the word.
	 */
	public int getBytesNumOfNodes()
	{
		if (_gNodesIds.size() > 0)
		{
			return (_tweetsNumbersOfNodes.size() + 1) * POINTER_SIZE;
		}
		return _tweetsNumbersOfNodes.size() * POINTER_SIZE;
	}
	
	/** Gets the number of bytes which all the  */
	public int getBytesOfTweetsNumbers()
	{
		int sum = 0;
		sum += _gNodesIds.size();
		for (TweetsNumbers value : _tweetsNumbersOfNodes.values())
		{
			sum += value.getSize();
		}
		return sum;
	}
	
	/** Increases the frequency of the word. */
	public void increaseFreq(int nodeId,int tweetNum)
	{
		/* in order to count only once an appearance of a word 
		 * which appears twice and more in the same tweet.
		 * As it is written in the exercise description:
		 * the frequency of the given word is the number of tweets
		 * containing this word. 
		 */
		if ( (_currentNodeId != nodeId) || (_currentTweetNum != tweetNum) ) 
		{
			_freq++;
			_currentNodeId = nodeId;
			_currentTweetNum = tweetNum;
		}
	}
	
	public void clearAll()
	{
		_gNodesIds.clearAll();
		_tweetsNumbersOfNodes.clear();
	}
	
	/** This private class containing the tweets' numbers for 
	 *  each node containing at least one tweet with the word from the wrapping class */
	private class TweetsNumbers
	{
		private int _prevTweetNum;
		private boolean _firstTweetNum;
		private GroupVarintEncoding _gTweetsNums;
		
		public TweetsNumbers()
		{
			_gTweetsNums = new GroupVarintEncoding();
			_firstTweetNum = true;
		}
			
		/** adds a tweet number. */
		public void addTweetNum(int currentTweetNum)
		{
			if (_firstTweetNum == true)
			{
				_prevTweetNum = _dictNum;
				_gTweetsNums.clearAll();
				if ((currentTweetNum - _prevTweetNum) >= 0)
				{
					_gTweetsNums.addToEncode(2 * (currentTweetNum - _prevTweetNum));
				}
			    else
			    {
			    	_gTweetsNums.addToEncode(2 * (-1) * (currentTweetNum - _prevTweetNum) - 1); // s1 - x
				}
				_firstTweetNum = false;
			}
			else
			{
		    	// In case a word appears more than once in a tweet, we don't want to save the tweet number twice and more.
		    	if (_prevTweetNum != currentTweetNum) 
		    	{
		    		_gTweetsNums.addToEncode(currentTweetNum - _prevTweetNum - 1);
		    	}
			}
			_prevTweetNum = currentTweetNum;
		}
		
		/** Gets the size of the encoded list of tweet numbers. */
		public int getSize()
		{
			return _gTweetsNums.size();
		}
		
		/** Writes the encoded list of tweets numbers .*/
		public void write(DataOutputStream outTweetsNumbersOfNodes)
		{
			_gTweetsNums.encodeGroups();
			for (int i = 0; i < _gTweetsNums.size(); i++)
			{
				try 
				{
					outTweetsNumbersOfNodes.writeByte(_gTweetsNums.getEncodedByte(i));
				} 
				catch (IOException e) {}
			}
		}
	}
}