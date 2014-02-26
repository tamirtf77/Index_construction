/** tamirtf77
 *  
 *  This class stores data about a node. 
 */

import java.io.DataOutputStream;
import java.io.IOException;
//import java.util.Iterator;
//import java.util.TreeSet;
import java.util.Iterator;
import java.util.TreeSet;

	public class NodeData
	{
		private static final byte CHAR_SIZE = 16;
		private static final String PUBLIC = "1";
		private static final String PRIVATE = "0";
		private static final char PADDING = '0';
		
		private int _id, _name, _numOfFollowers,_prevFollower;
		private char _nameAndPrivacy;
		private TreeSet<Integer> _followings;
		private GroupVarintEncoding _gFollowing, _gFollowers, _gTweetsPointers, _gTweet;
		private boolean _firstFollower;
		
		public NodeData()
		{
        	_followings = new TreeSet<Integer>();
			_gFollowing = new GroupVarintEncoding();
			
			_gFollowers = new GroupVarintEncoding();
			
			_gTweetsPointers = new GroupVarintEncoding();
			_gTweet = new GroupVarintEncoding();
		}
		
		public void init(int id, String name, String privacy)
		{
			_id = id;
			_name = Integer.parseInt(name,2);
        	name = paddingAndAddsPrivacy(name,CHAR_SIZE,privacy);
        	_nameAndPrivacy = (char)Integer.parseInt(name, 2);
        	_followings.clear();
			_gFollowing.clearAll();
			
			_gFollowers.clearAll();
			_firstFollower = true;
			_prevFollower = _id;
			_numOfFollowers = 0;
			
			_gTweetsPointers.clearAll();
			_gTweet.clearAll();
		}
		
		/** Gets the Id. */
		public int getId()
		{
			return _id;
		}
		
		/** Gets the name (name's number). */
		public int getName()
		{
			return _name;
		}
		
		/** Manipulates the name, in order to save the node's privacy 
		 *  in the second bit from left.
		 */
		private String paddingAndAddsPrivacy(String binaryNum,int size,String privacy)
		{
			int remainder = size - binaryNum.length();
			for (int i=1; i <=remainder; i++)
				{
					if (i == remainder-1)
					{
						if (privacy.equals(PUBLIC))
						{
							binaryNum = PUBLIC + binaryNum;
						}
						else //privacy = PRIVATE
						{
							binaryNum = PRIVATE + binaryNum;
						}
					}
					else
					{
						binaryNum = PADDING + binaryNum;
					}
			}
			return binaryNum;
		}
				
		public void addFollowing(int following)
		{
			_followings.add(following);
		}
		
		/** Adds a node id which follows the node. */
		public void addFollower(int follower)
		{
			if (_firstFollower == true)
			{
				_gFollowers.clearAll();
				if ((follower - _prevFollower) >=0)
				{
					_gFollowers.addToEncode(2 * (follower - _prevFollower));
				}
			    else
			    {
			    	_gFollowers.addToEncode(2 * (-1) * (follower - _prevFollower) - 1); // s1 - x
				}
				_firstFollower = false;
			}
			else
			{
				_gFollowers.addToEncode(follower - _prevFollower - 1);
			}
			_prevFollower = follower;
			_numOfFollowers++;
		}
		
		/** Adds a tweet id. */
		public void addTweetsPointer(int prevIndex,int currentIndex,boolean b)
		{
			if (b == true)
			{
				_gTweetsPointers.clearAll();
				if ((currentIndex - prevIndex) >=0)
				{
					_gTweetsPointers.addToEncode(2 * (currentIndex - prevIndex));
				}
			    else
			    {
			    	_gTweetsPointers.addToEncode(2 * (-1) * (currentIndex - prevIndex) - 1); // s1 - x
				}
			}
			else
			{
				_gTweetsPointers.addToEncode(currentIndex - prevIndex - 1);
			}
		}
		
		/** Adds a tweet.*/
		public void addTweet(String[] words)
		{
			_gTweet.clearAll();
			for (int i=0; i < words.length-1; i++)
			{
				encodingTweet(Integer.parseInt(words[i]),Integer.parseInt(words[i+1]));
			}
			_gTweet.encodeGroups();
		}
			
		/** Encoding the following. */
		private void encodingFollows(TreeSet<Integer> follows, GroupVarintEncoding gFollows)
		{
			Iterator<Integer> it = follows.iterator();
			int current,prev;
			if (follows.size() > 0)
			{
				prev = it.next();
				if ((prev - _id) >=0)
				{
					gFollows.addToEncode(2 * (prev - _id));
				}
			    else
			    {
			    	gFollows.addToEncode(2 * (-1) * (prev - _id) - 1); // s1 - x
				}
				while (it.hasNext())
				{
					current = it.next();
					gFollows.addToEncode(current-prev-1);
					prev = current;
				}
				gFollows.encodeGroups();
			}
		}
		
		/** Encoding the tweet. */
		private void encodingTweet(int word1, int word2) 
		{
			if ((word2 - word1) >= 0)
			{
				_gTweet.addToEncode(2 * (word2 - word1));
			}
			else
			{
			    _gTweet.addToEncode(2 * (-1) * (word2 - word1) - 1); // s1 - x
			}
		}

		/** Gets size in bytes of a an encoding list. */
		public int getSize(String s)
		{
			if (s.equals(IndexWriter.FOLLOWING))
			{
				return _gFollowing.size();
			}
			if (s.equals(IndexWriter.FOLLOWERS))
			{
				return _gFollowers.size();
			}
			if (s.equals(IndexWriter.TWEETS))
			{
				return _gTweet.size();
			}
			if (s.equals(IndexWriter.TWEETS_INDEX))
			{
				return _gTweetsPointers.size();
			}
			return 0;
		}
		
		/** Writes the data about a node, its following and followers.*/
		public void writeNodeAndFollows(DataOutputStream out,DataOutputStream outFollowing,int prevFollowingBytes,
								 DataOutputStream outFollowers,int prevFollowersBytes,int prevTweetsBytes)
		{
			encodingFollows(_followings,_gFollowing);
			try 
			{
				out.writeChar(_nameAndPrivacy);
				out.writeInt(_followings.size());
				out.writeInt(_numOfFollowers);
				out.writeInt(prevFollowingBytes);
				_gFollowers.encodeGroups();
				for (int i = 0; i < _gFollowing.size(); i++)
				{
					outFollowing.writeByte(_gFollowing.getEncodedByte(i));
				}
				out.writeInt(prevFollowersBytes);
				for (int i = 0; i < _gFollowers.size(); i++)
				{
					outFollowers.writeByte(_gFollowers.getEncodedByte(i));
				}
				out.writeInt(prevTweetsBytes);
			}
	        catch (IOException e) {}
	    }
		
		/** Writes an encoding tweet.*/
		public void writeTweet(DataOutputStream outTweets) 
		{
			try
			{
				for (int i = 0; i < _gTweet.size(); i++)
				{
					outTweets.writeByte(_gTweet.getEncodedByte(i));
				}
			}
			catch (IOException e){} 
		}
		
		/** Writes an the tweets pointers (each pointer point the beginning of the tweet).*/
		public void writeTweetsPointers(DataOutputStream outTweetsIndexes) 
		{
			_gTweetsPointers.encodeGroups();
			try
			{
				for (int i = 0; i < _gTweetsPointers.size(); i++)
				{
					outTweetsIndexes.writeByte(_gTweetsPointers.getEncodedByte(i));
				}
			}
			catch (IOException e){} 
		}
}