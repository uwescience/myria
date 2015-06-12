import uploadtest as ut
import numpy as np
import sys
import benchmarksDriver as bd

files = [
    "ocean_seq.csv"
    ]

def run_em_test(filename, workers):
    times = []
    name = filename.split(".")[0].replace("-","_")
    try:
        name = ut.upload_parallel(filename, workers=workers)
    except:
        None
    bd.CopyToPoints(name)
    bd.pad_points()
    bd.copy_points()
    bd.upload_components()
    # Copy over components and try iteration
    bd.astro_components_to_components()
    try:
        times.append(bd.EMStep())
    except:
        None 
    # Copy over components and try iteration        
    bd.astro_components_to_components()    
    try:
        times.append(bd.EMStep())
    except:
        None
    # Copy over components and try iteration        
    bd.astro_components_to_components()        
    try:
        times.append(bd.EMStep())
    except:
        None 
    np.savetxt(filename+"times_w" + str(workers) +".csv", np.array(times),delimiter=',' )
    return times

if __name__ == "__main__":
    alltimes = []
    for filename in files:
        alltimes.append(run_em_test(filename, int(sys.argv[1])))
