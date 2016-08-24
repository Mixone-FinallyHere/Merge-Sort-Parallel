package merge.sort.parallel;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * @date 17/12/15
 * @netid mteroles
 * @matricule 356914
 * @anne BA2 INFO
 * @author Miguel Terol Espino
 * @param <T>
 */
public class MergeSortParallel<T extends Comparable> {    
    
    /*
    This is an override of the println methode more or less 
    to be able to differentiate between the different threads on console output.
    */
    public void printlnOverride(String toPrint, int order ) {
        switch (order) {
            case 1:
                System.out.println(toPrint);                
                break;
            default:
                String tabulation= "";
                for (int i=0; i < order-9; i++) {
                    tabulation += "\t\t";
                }
                System.out.println(tabulation + toPrint);
                break;
        }
    }    
    public class Seamstress extends Thread {
        /*
        This class is a thread built for mergesorting, hence it being internal to the mergesort class
        */
        private final T[] data;
        public T[] get() {
            printlnOverride("Thread "+this.getName()+" state: "+this.getState(), (int) this.getId());
            return data;
        }
        public void mergeSort(T[] array) throws InterruptedException {
            if (array.length > 1) {
                printlnOverride(this.getName()+" array to sort :"+Arrays.toString(array), (int) this.getId());
                T[] left = Arrays.copyOfRange(array, 0, array.length/2);
                T[] right = Arrays.copyOfRange(array, left.length, array.length);
                if (array.length < 100) {
                    sequential_mergesort(left);
                    sequential_mergesort(right);
                    merge(array, left, right);
                }
                else {
                    Seamstress Lowtilda = new Seamstress(left, "Lowtilda child"); 
                    Seamstress Hightilda = new Seamstress(right, "Hightilda child");
                    Lowtilda.join();
                    Hightilda.join();
                    merge(array, left, right);
                }                
            }
        }       
        public void merge(T[] result, T[] left, T[] right) {
            printlnOverride(this.getName()+" right: "+Arrays.toString(right), (int) Thread.currentThread().getId());
            printlnOverride(this.getName()+" left: "+Arrays.toString(left), (int) Thread.currentThread().getId());
            int k = 0, j = 0;
            for (int i = 0; i < result.length; i++) {
                if (j < left.length && k < right.length) {
                    result[i] = left[j].compareTo(right[k]) < 1 ? left[j++] : right[k++];                    
                } else if (k >= right.length) {
                    result[i] = left[j++];
                } else if (j >= left.length) {
                    result[i] = right[k++];
                }
                printlnOverride("Result["+i+"] has become: "+result[i], (int) Thread.currentThread().getId());
            }
            printlnOverride(this.getName()+" result of merge: "+Arrays.toString(result), (int) Thread.currentThread().getId());
        }        
        Seamstress(T[] array, String name) throws InterruptedException {
            this.setName(name);
            printlnOverride("Thread "+this.getName()+" state: "+this.getState()+"\tID: "+(int) this.getId(), (int) Thread.currentThread().getId());
            data = array;
            this.start();
        }
        @Override
        public void run() {
            try {
                mergeSort(data);
            } catch (InterruptedException ex) {
                Logger.getLogger(MergeSortParallel.class.getName()).log(Level.SEVERE, null, ex);
            }
            printlnOverride("Thread "+this.getName()+" state: "+this.getState(), (int) Thread.currentThread().getId());
        }    
    }
   
    public void sequential_mergesort(T vec[]) {
        /*
        This is the given algorithm translated into Java code.
        */
        if (vec.length > 1) {
            printlnOverride("Sequential Merge Sort of: "+Arrays.toString(vec), (int) Thread.currentThread().getId());
            T[] left = Arrays.copyOfRange(vec, 0, vec.length/2);
            T[] right = Arrays.copyOfRange(vec, left.length, vec.length);
            sequential_mergesort(left);
            sequential_mergesort(right);            
            @SuppressWarnings("unchecked")  // We know that it will be a number, so in theory no need for warning.                                             
            int i = 0, j = 0, k = 0;
            while (i < left.length && j < right.length) {
                vec[k++] = left[i].compareTo(right[j]) == -1 ? left[i++] : right[j++];                 
            }
            System.arraycopy(left, i, vec, k, left.length-i );
            System.arraycopy(right, j, vec, k, right.length - j);
            printlnOverride("Treated array is: "+Arrays.toString(vec), (int) Thread.currentThread().getId());
        }
    }
    
    public T[] finalMerge(T[] down, T[] upp) {
        T[] result = (T[]) new Comparable[upp.length+down.length];
        int i=0, j=0, k=0;
        while (i < down.length && j < upp.length) {
            result[k++] = down[i].compareTo(upp[j]) == -1 ? down[i++] : upp[j++];                        
        }
        System.arraycopy(down, i, result, k, down.length - i);
        System.arraycopy(upp, j, result, k, upp.length - j);
        return result;        
    }
    
    public void parallel_mergesort(T[] data) throws InterruptedException { 
        printlnOverride("Merge sorting array:\t"+Arrays.toString(data), (int) Thread.currentThread().getId());
        printlnOverride("Parallel Merge Sort:", (int) Thread.currentThread().getId());
        long start = System.currentTimeMillis();    // Time controll
        T[] lower = (T[]) new Comparable[data.length / 2];
        T[] upper = (T[]) new Comparable[data.length - lower.length];
        System.arraycopy(data, 0, lower, 0, lower.length);
        System.arraycopy(data, lower.length, upper, 0, upper.length);
        // Divide array in two
        
        // Threading setup
        Seamstress low = new Seamstress(lower, "Lowtilda"); 
        Seamstress high = new Seamstress(upper, "Hightilda");
        // Join when done
        low.join(); 
        high.join();
        
        // Evaluate resulting arrays
        printlnOverride("Merging thread's results....", (int) Thread.currentThread().getId());
        T[] result = finalMerge(low.get(), high.get()); // Get thread resulting aarays
        printlnOverride("Done.", (int) Thread.currentThread().getId());        
        long stop = System.currentTimeMillis(); // Time control
        long elapsed = stop - start;
        printlnOverride("Sorted array;\t"+Arrays.toString(result), (int) Thread.currentThread().getId());  
        printlnOverride("Parallel Merge sort took: "+elapsed+" miliseconds", (int) Thread.currentThread().getId());
        
        
    }

    /**
     * @param args the command line arguments
     * @throws java.lang.InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        MergeSortParallel<Integer> controller = new MergeSortParallel<>();
        int numTests = 5;
        for (int i=0; i < numTests; i++) {
            MSTesting tester = new MSTesting(15, 100);
            Integer[] test = tester.getRandomnArray();
            System.out.println("==================================================================================================================\n\nTest number "+(i+1));
            controller.parallel_mergesort(test); 
        }
                   
    }
    
    public static class MSTesting {
        /*
        Class for automatation of tests.
        */
        private int size;
        private int range;        
        MSTesting(int Nsize, int Nrange) {
            size = Nsize;
            range = Nrange;
        }
        public Integer[] getRandomnArray() {
            Integer test[] = new Integer[size];
            for(int i=0; i < size; i++) {
                test[i] = new Integer((int)(Math.random() * range+1));
            }
            return test;
        }    
    }
}


