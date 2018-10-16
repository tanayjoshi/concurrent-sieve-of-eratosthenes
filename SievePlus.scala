/** A concurrent implementation of the Sieve of Eratosthenes */

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import ox.cads.util.Profiler
import ox.cads.util.ThreadUtil
import ox.cads.collection.LockFreeStack
import ox.cads.collection.Pool
import ox.cads.util.ThreadID

object SievePlus{
  def main(args: Array[String]) = {
    assert(args.length == 1, "must have one argument")
    val t0 = java.lang.System.currentTimeMillis()
    val threads = 4
    val N = args(0).toInt // number of primes required
    val primes = new AtomicIntegerArray(N+threads-1) // will hold the primes
    primes.set(0, 2)
    var nextSlot = new AtomicInteger(1) // next free slot in primes
    var next = new AtomicInteger(3) // next candidate prime to consider
    val currentValue = new AtomicIntegerArray(threads)

	def worker = {    
	while(nextSlot.get<N){
// Test if next is prime; 
// invariant: next is coprime with primes[0..i) && p = primes(i)
  	  
      		var i = 0
		var p = primes.get(i)
		val nextValue = next.getAndIncrement

		currentValue.set(ThreadID.get % threads, nextValue)
		for (i <- 0 until threads)	{
			var safe = false
			while(!safe)
			{
				val x = currentValue.get(i) : Long
				val sqVal = x*x
				if (sqVal > nextValue)
					safe = true
			}
		}      
		
		while(p*p<=nextValue && nextValue%p != 0){ 
			i += 1; 
			p=0; 
			while(p==0) {
			p = primes.get(i) 
			}
		}
      	
		if(p*p>nextValue){ // next is prime
	  	val nextSlotValue = nextSlot.getAndIncrement;
		var prevEntry = 0;
		while(prevEntry == 0)	{
			prevEntry = primes.get(nextSlotValue-1);
		}
		if (prevEntry < nextValue)	{	
			primes.set(nextSlotValue, nextValue);
		}
		else	{
			primes.set(nextSlotValue, prevEntry);
			var done = false;
			var currentEntry = 0;
			var nextEntry = 0;
			var currentSlot = nextSlotValue-1;
			while (!done)	{
				currentEntry = primes.get(currentSlot);
				nextEntry = primes.get(currentSlot + 1);
				if (currentEntry != nextEntry)	{
					currentSlot +=1
				}
				else {
					prevEntry = primes.get(currentSlot-1);
					if (prevEntry < nextValue)	{
						done = primes.compareAndSet(currentSlot, currentEntry, nextValue)
					} else {
						currentSlot -= (if(primes.compareAndSet(currentSlot,currentEntry, prevEntry)) 1 else 0)
					}
				}
			}
		}
	  	//primes.set(nextSlot.get, nextSlotValue);
    	}
    }
}
	ThreadUtil.runSystem(threads, worker)	

    println(primes.get(N-1))
    println("Time taken: "+(java.lang.System.currentTimeMillis()-t0))
    // About 2.7 secs for N = 1,000,000; answer: 15,485,863
  }
}
