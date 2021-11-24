import multiprocessing
import os                                              
   
# Creating the tuple of all the processes which can be run in parallel
all_parallel_processes = ('save_data_FandO.py', 'save_data_stock1.py', 'save_data_stock2.py')     
                                                                                                               
next_run = ('script_D.py')
                                                  
# This block of code enables us to call the script from command line.                                                                                
def execute(process):                                                             
    os.system(f'python {process}')                                       
                                                                                
                                                                                
process_pool = multiprocessing.Pool(processes = 3)                                                        
process_pool.map(execute, all_parallel_processes)
# process_pool.map(execute, next_run)