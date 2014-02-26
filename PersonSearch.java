/** tamirtf77
 *  This class implements search methods of persons. 
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Vector;


public class PersonSearch 
{
	
	private IndexReader _ir = new IndexReader();
	private int _numOfNodes,_numOfTweets, _numOfWords;
	private boolean _readNumOfNodes = false, _readNumOfTweets = false;
	private static final int NUM_OF_NODES_BYTES = 4;
	private static final String FOR_READING = "r";
	private static final int BASE = 2;
	private static final String WORD_SEPERATOR = " ";
	private TreeMap<String,Integer> _wordOccur;
	
	/**
	* Returns a list of most highly ranked nodes as results
	* using the ranking function impSearch
	* The list should be sorted by node id (from smallest to largest)
	*/
	public Enumeration<Integer> impSearch(int nodeId, Enumeration<String> words)
	{
		Enumeration<Map.Entry<Integer,String>> tweets;
		Entry<Integer,String> entry;
		String word;
		TreeMap<Integer,TreeSet<String>> tweetsOfNodeJ = new TreeMap<Integer,TreeSet<String>>(); 
		TreeMap<Integer,Integer> nodesRank = new TreeMap<Integer,Integer>();
		int score;
		while ( (words != null) && (words.hasMoreElements()) )
		{
			word = words.nextElement();
			tweets = _ir.getTweetsByWord(word,nodeId);
			while ((tweets != null) && (tweets.hasMoreElements()))
			{
				entry =  tweets.nextElement();
				if (tweetsOfNodeJ.containsKey(entry.getKey()))
				{
					tweetsOfNodeJ.get(entry.getKey()).add(word);
				}
				else
				{
					TreeSet<String> set = new TreeSet<String>();
					set.add(word);
					tweetsOfNodeJ.put(entry.getKey(), set);
				}
			}
		}

		for (Map.Entry<Integer, TreeSet<String>> e : tweetsOfNodeJ.entrySet())
		{
			if ( (score = (e.getValue().size() * _ir.getInDegree(e.getKey()))) > 0)
			{
				nodesRank.put(e.getKey(), score);
			}
		}
		return getHighRanked(nodesRank);
	}
	
	/** Gets the best keys i.e. they have the highest value. */
	private <T extends Number & Comparable<T>> Enumeration<Integer> getHighRanked(TreeMap<Integer,T> nodesRank)
	{
		TreeSet<Integer> highRanked = new TreeSet<Integer>();
		T highValue = null;
		for (Entry<Integer, T> e : nodesRank.entrySet()) 
		{
			if (highValue == null)
			{
				highRanked.add(e.getKey());
				highValue = e.getValue();
			}
			else if (e.getValue().equals(highValue))
			{
				highRanked.add(e.getKey());
			}
			else if ( (e.getValue().compareTo(highValue)) > 0 )
			{
				highRanked.clear();
				highRanked.add(e.getKey());
				highValue = e.getValue();
			}
		}
		return Collections.enumeration(highRanked);
	}
	
	/** A class in order to count the number of occurrences of a tweet.*/
	private class CountSameTweet
	{
		private int _counter;
		private boolean _alreadyAdded;
		
		public CountSameTweet()
		{
			_counter = 0;
			_alreadyAdded = false;
		}
		
		public int getCounter()
		{
			return _counter;
		}
		
		public boolean isAlreadyAdded()
		{
			return _alreadyAdded;
		}
		
		public void increaseCounter()
		{
			_counter++;
		}
		
		public void setAlreadyAdded()
		{
			_alreadyAdded = true;
		}
	}

	/**
	* Returns a list of most highly ranked nodes as results
	* using the ranking function tfIdfSearch
	* The list should be sorted by node id (from smallest to largest)
	*/
	public Enumeration<Integer> tfIdfSearch(int nodeId, Enumeration<String> words)
	{
		// QUERY = words, DOC = tweet
		Vector<String> wordsCopy = new Vector<String>();
		TreeMap<String,Double> weightWord = new TreeMap<String,Double>();
		TreeMap<Integer,TreeMap<String,CountSameTweet>> tweetsOfNodeId = new TreeMap<Integer,TreeMap<String,CountSameTweet>>();
		TreeMap<Integer,Double> nodesRank = new TreeMap<Integer,Double>();
		
		while ( (words != null) && (words.hasMoreElements()) )
		{
			wordsCopy.add(words.nextElement());
		}
		calWeightWordFreq(wordsCopy,weightWord);
		getTweetsOfFollowingNode(nodeId,wordsCopy,tweetsOfNodeId);
		calTfIdf(wordsCopy,weightWord,tweetsOfNodeId,nodesRank);
		return getHighRanked(nodesRank);
	}
	
	/** Calculates TfIdf of a tweet. */
	private void calTfIdf(Vector<String> wordsCopy,TreeMap<String,Double> weightWord, TreeMap<Integer,TreeMap<String,CountSameTweet>> tweetsOfNodeId,
						  TreeMap<Integer,Double> nodesRank)
	{
		String[] wordsOfTweet;
		double sumOfNumerator, sumOfDenominator, score;
		int freqInTweet = 0;
		
		for (Map.Entry<Integer,TreeMap<String,CountSameTweet>> e : tweetsOfNodeId.entrySet()) 
		{
			sumOfNumerator = sumOfDenominator = score = 0;
			for (Map.Entry<String, CountSameTweet> ee: e.getValue().entrySet())
			{
				wordsOfTweet = ee.getKey().split(WORD_SEPERATOR);
				for (int k = 0; k < wordsCopy.size(); k++)
				{
					if ( (freqInTweet = getFreqInTweet(wordsCopy.get(k),wordsOfTweet)) > 0 )
					{
						sumOfNumerator += (weightWord.get(wordsCopy.get(k)) * (1 + (Math.log10(freqInTweet) / Math.log10(BASE))));
					}
				}
				for (int k = 0; k < wordsOfTweet.length; k++)
				{
					sumOfDenominator += Math.pow( (1 + (Math.log10(getFreqInTweet(wordsOfTweet[k],wordsOfTweet)) / Math.log10(BASE))), BASE);
				}
				if (sumOfDenominator == 0) { sumOfDenominator = 1; } //avoid division by zero 
				score += (ee.getValue().getCounter() * (sumOfNumerator / Math.sqrt(sumOfDenominator)));
				sumOfNumerator = sumOfDenominator = 0;
		    }
			if (nodesRank.containsKey(e.getKey()))
			{
				nodesRank.put(e.getKey(), (nodesRank.get(e.getKey()) + score) );
			}
			else
			{
				if (score > 0)
				{
					nodesRank.put(e.getKey(), score);
				}
			}
		}
	}
	
	/** Gets the tweets of following nodes. */
	private void getTweetsOfFollowingNode(int nodeId,Vector<String> wordsCopy,TreeMap<Integer,TreeMap<String,CountSameTweet>> tweetsOfNodeId)
	{
		Enumeration<Map.Entry<Integer,String>> tweets;
		Entry<Integer,String> entry = null;
		for (int i = 0; i < wordsCopy.size(); i++)
		{
			tweets = _ir.getTweetsByWord(wordsCopy.get(i),nodeId);
			while ((tweets != null) && (tweets.hasMoreElements()))
			{
				entry =  tweets.nextElement();
				if (tweetsOfNodeId.containsKey(entry.getKey()))
				{
					if ( (tweetsOfNodeId.get(entry.getKey()).containsKey(entry.getValue())) &&
						 (tweetsOfNodeId.get(entry.getKey()).get(entry.getValue()).isAlreadyAdded() == false) )	
					{
						tweetsOfNodeId.get(entry.getKey()).get(entry.getValue()).increaseCounter();
					}
					else if (tweetsOfNodeId.get(entry.getKey()).containsKey(entry.getValue()) == false)	
					{
						CountSameTweet c = new CountSameTweet();
						c.increaseCounter();
						tweetsOfNodeId.get(entry.getKey()).put(entry.getValue(), c);
					}
				}
				else
				{
					CountSameTweet c = new CountSameTweet();
					c.increaseCounter();
					TreeMap<String,CountSameTweet> m = new TreeMap<String,CountSameTweet>();
					m.put(entry.getValue(),c);
					tweetsOfNodeId.put(entry.getKey(), m);
				}
			}
			
			for (Integer j: tweetsOfNodeId.keySet())
			{
				for (String s: tweetsOfNodeId.get(j).keySet())
				{
					tweetsOfNodeId.get(j).get(s).setAlreadyAdded();
				}
			}
		}
	}
	
	/** Gets the frequency of word in a tweet. */
	private int getFreqInTweet(String word, String[] wordsOfTweet)
	{
		int counter = 0;
		for (int i = 0; i < wordsOfTweet.length; i++)
		{
			if (wordsOfTweet[i].equals(word))
			{
				counter++;
			}
		}
		return counter;
	}
	
	/** Calculates the the weight frequency score of a word for TfIdf, */
	private void calWeightWordFreq(Vector<String> wordsCopy,TreeMap<String,Double> weightWord)
	{
		int numOfTweets = getNumOfTweetsAndWords();
		int freq = 0;
		double denominator = Math.log10(BASE);
		for (int i = 0; i < wordsCopy.size(); i++)
		{
			if ( (freq = _ir.getWordFrequency(wordsCopy.get(i))) > 0 )
			{
				weightWord.put(wordsCopy.get(i),Math.log10(1 +((double)numOfTweets/(double)freq)) /  denominator);
			}
			else
			{
				weightWord.put(wordsCopy.get(i),0.0);
			}
		}
	}
	
	/** Gets the number of nodes in the social network. */
	private int getNumOfNodes()
	{
		if (_readNumOfNodes == false)
		{
			File file = new File(IndexWriter.NODES_INDEX);
			RandomAccessFile inNodesIndex = open(file);
			try 
			{
				inNodesIndex.seek(file.length() - NUM_OF_NODES_BYTES);
				_numOfNodes = inNodesIndex.readInt();
				_readNumOfNodes = true;
			} 
			catch (IOException e) {}
			close(inNodesIndex);
		}
		return _numOfNodes;
	}
	
	/** Gets the number of tweets, number of words in all tweets and 
	 * count for each word how many it appears in all the words in all the tweets. */
	private int getNumOfTweetsAndWords() 
	{
		int numOfNodes = getNumOfNodes();
		if (_readNumOfTweets == false)
		{
			_numOfTweets = 0; _numOfWords = 0;
			_wordOccur = new TreeMap<String,Integer>();
			String[] tweetWords;
			Enumeration<String> tweets;
			for (int i = 0; i < numOfNodes; i++)
			{
				tweets = _ir.getTweets(i);
				while (tweets.hasMoreElements())
				{ 
					_numOfTweets++; 
					tweetWords = tweets.nextElement().split(WORD_SEPERATOR);
					_numOfWords += tweetWords.length;
					for (int j = 0; j < tweetWords.length; j++)
					{
						if (_wordOccur.containsKey(tweetWords[j]))
						{
							_wordOccur.put(tweetWords[j], (_wordOccur.get(tweetWords[j]) + 1) );
						}
						else
						{
							_wordOccur.put(tweetWords[j], 1);
						}
					}
				}
			}
			_readNumOfTweets = true;
		}
		return _numOfTweets;
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
	
	
	
	
	/**
	* Returns a list of most highly ranked nodes as results
	* using the ranking function creativeSearch
	* The list should be sorted by node id (from smallest to largest)
	*/
	public Enumeration<Integer> creativeSearch(int nodeId, Enumeration<String> words)
	{
		Vector<String> wordsCopy = new Vector<String>();
		TreeMap<Integer,TreeSet<String>> tweetsOfNodeId = new TreeMap<Integer,TreeSet<String>>();
		TreeMap<Integer,Double> nodesRank = new TreeMap<Integer,Double>();
		double sigma = 0.5, followsRatio = 0, scoreLanguageModel = 0;
		while ( (words != null) && (words.hasMoreElements()) )
		{
			wordsCopy.add(words.nextElement());
		}
		getWordsInTweetsOfNodeJ(nodeId,wordsCopy,tweetsOfNodeId);
		//calFollowsRatio(tweetsOfNodeId,nodesRank,sigma);
		getNumOfTweetsAndWords();
		for (Map.Entry<Integer,TreeSet<String>> e : tweetsOfNodeId.entrySet()) 
		{
			followsRatio = calFollowsRatio(e.getKey(),sigma);
			scoreLanguageModel = calSumOfLanguageModelNodeJ(e.getKey(),e.getValue(),1-sigma);
			if ( (scoreLanguageModel > 0) && ( (scoreLanguageModel + followsRatio) > 0) )
			{
				nodesRank.put(e.getKey(), (scoreLanguageModel + followsRatio) );
			}
		}
		return getHighRanked(nodesRank);
	}
	
	/** Gets j and words W_j of j such that 
	 *   a. i could see the tweets of j .
	 *   b. j has at least one tweet with a word w belongs to W (wordsCopy).
	 */
	private void getWordsInTweetsOfNodeJ(int nodeId,Vector<String> wordsCopy,TreeMap<Integer,TreeSet<String>> tweetsOfNodeId)
	{
		Enumeration<Map.Entry<Integer,String>> tweets;
		Entry<Integer,String> entry = null;
		for (int i = 0; i < wordsCopy.size(); i++)
		{
			tweets = _ir.getTweetsByWord(wordsCopy.get(i),nodeId);
			while ((tweets != null) && (tweets.hasMoreElements()))
			{
				entry =  tweets.nextElement();
				if (tweetsOfNodeId.containsKey(entry.getKey()))
				{
					tweetsOfNodeId.get(entry.getKey()).add(wordsCopy.get(i));
				}
				else
				{
					TreeSet<String> set = new TreeSet<String>();
					set.add(wordsCopy.get(i));
					tweetsOfNodeId.put(entry.getKey(), set);
				}
			}
		}
	}

	/** Calculates the follows ratio i.e. sigma *(number of followers / number of following) for node j */
	private double calFollowsRatio(int nodeJ, double sigma) 
	{
		int outDegree = _ir.getOutDegree(nodeJ);
		if (outDegree == 0)
		{
			return 0.0;
		}
		return (sigma * ((double)_ir.getInDegree(nodeJ) / (double)outDegree));
	}
	
	/** Gets the tweets, which contains at least one word belongs to W_j, of node k and node k follows node j. */
	private void getTweetsOfFollowerNodeK(int nodeJ,Vector<String> wordsOfNodeJ,TreeMap<Integer,Vector<String>> tweetsOfNodeK)
	{
		Enumeration<Integer> followers = _ir.getFollowers(nodeJ);
		Enumeration<String> tweets;
		int follower;
		String tweet;
		String[] tweetWords;
		boolean tweetHasWordOfJ = false; // if a tweet of k has at least one word of j then it is enough and we could add this tweet for calculating the score.
		while ( (followers != null) && (followers.hasMoreElements()) )
		{
			follower = followers.nextElement();
			tweets = _ir.getTweets(follower);
			while ( (tweets != null) && (tweets.hasMoreElements()) )
			{
				tweet = tweets.nextElement();
				tweetWords = tweet.split(WORD_SEPERATOR);
				for (int i = 0; (i < tweetWords.length) && (tweetHasWordOfJ == false); i++)
				{
					for (int l = 0; (l < wordsOfNodeJ.size()) && (tweetHasWordOfJ == false); l++)
					{
						if (tweetWords[i].equals(wordsOfNodeJ.get(l)))
						{
							if (tweetsOfNodeK.containsKey(follower))
							{
								tweetsOfNodeK.get(follower).add(tweet);
							}
							else
							{
								Vector<String> vec = new Vector<String>();
								vec.add(tweet);
								tweetsOfNodeK.put(follower, vec);
							}
							tweetHasWordOfJ = true;
						}
					}
				}
				tweetHasWordOfJ = false;
			}
		}
	}
	
	/** Calculates the sum of the Language Model's score for all tweets, 
	 * which contains at least one word belongs to W_j, of nodes k and node k follows node j. */
	private double calSumOfLanguageModelNodeJ(int nodeJ, TreeSet<String> words, double sigmaC)
	{
		Iterator<String> it;
		TreeMap<Integer,Vector<String>> tweetsOfNodeK = new TreeMap<Integer,Vector<String>>();
		Vector<String> wordsOfNodeJ = new Vector<String>();
		TreeMap<String,Double> probInAll = new TreeMap<String,Double>();
		double sigmaLanguageModel = 0.5, score = 0.0;
		
		it = words.iterator();
		while (it.hasNext())
		{
			wordsOfNodeJ.add(it.next());
		}
		getTweetsOfFollowerNodeK(nodeJ,wordsOfNodeJ,tweetsOfNodeK);
		getProbabilityInAll(wordsOfNodeJ,probInAll,1-sigmaLanguageModel);
		for (Entry<Integer, Vector<String>> e : tweetsOfNodeK.entrySet()) 
		{
			score += calLanguageModelNodeK(wordsOfNodeJ,e.getValue(),probInAll,sigmaLanguageModel);
		}
		return (sigmaC * score);
	}

	/** Gets the probability of word in all the social network. */
	private void getProbabilityInAll(Vector<String> wordsOfNodeJ,TreeMap<String, Double> probInAll,double sigmaLanguageModel)
	{
		for (int i = 0; i < wordsOfNodeJ.size(); i++)
		{
			if (probInAll.containsKey(wordsOfNodeJ.get(i)) == false)
			{
				probInAll.put(wordsOfNodeJ.get(i),(sigmaLanguageModel * (double)_wordOccur.get(wordsOfNodeJ.get(i))/ (double)_numOfWords) );
			}
		}
	}

	/** Calculates the Language Model for words w_j in W_j for all tweets of k. */
	private double calLanguageModelNodeK(Vector<String> wordsOfNodeJ,
			       Vector<String> TweetsOfNodeK,TreeMap<String,Double> probInAll,
			       double sigmaLanguageModelC) 
	{
		Iterator<String> it = TweetsOfNodeK.iterator();
		String[] wordsOfTweet;
		double probInTweet;
		TreeMap<String,Double> scoreOfWordInTweet = new TreeMap<String,Double>();
		double scoreOfTweet = 1.0, scoreOfAllTweets = 0.0;
		boolean found = false;
		while (it.hasNext())
		{
			wordsOfTweet = it.next().split(WORD_SEPERATOR);
			for (int i = 0; i < wordsOfNodeJ.size(); i++)
			{
				for (int j = 0; (j < wordsOfTweet.length) && (found == false); j++)
				{
					if (wordsOfNodeJ.get(i).equals(wordsOfTweet[j]))
					{
						probInTweet = sigmaLanguageModelC * (double)getFreqInTweet(wordsOfTweet[j], wordsOfTweet)/(double)wordsOfTweet.length;
						scoreOfWordInTweet.put(wordsOfNodeJ.get(i), (probInTweet + probInAll.get(wordsOfNodeJ.get(i))));
						found = true;
					}
				}
				found = false;
			}
			
			for (Map.Entry<String, Double> e : scoreOfWordInTweet.entrySet())
			{
				scoreOfTweet *= e.getValue();
			}
			
			scoreOfAllTweets += scoreOfTweet;
			scoreOfTweet = 1.0;
			scoreOfWordInTweet.clear();
		}
		return scoreOfAllTweets;
	}
}