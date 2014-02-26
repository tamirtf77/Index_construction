/** tamirtf77
 *  
 */
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;


public class Main
{
		public static void main(String[] args)
		{
			long before = System.currentTimeMillis();
			IndexWriter s = new IndexWriter();
			s.createIndex();
			long after = System.currentTimeMillis();
			System.out.println("time to create is "+ (after - before));
			
			//Runtime runtime = Runtime.getRuntime();
			//System.out.println("maxMemory is "+ runtime.maxMemory());
			//System.out.println("totalMemory is "+ runtime.totalMemory());
			//System.out.println("freeMemory is "+ runtime.freeMemory());
            
			IndexReader ir = new IndexReader();
			PersonSearch p = new PersonSearch();
			Vector<String> words = new Vector<String>();
			words.add("great");
			words.add("a");
			Enumeration<Integer> eee = p.tfIdfSearch(2, words.elements());
			while ( (eee != null) && (eee.hasMoreElements()))
			{
				System.out.println("ttthe node id is " + eee.nextElement());
			}
		}
}
