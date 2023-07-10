from distlink import COrbitData, MOID_fast
from bin_to_df import bin_to_df
import rebound
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import schwimmbad
import functools

def per_calc(i, astdys):
    print(i)
    dist_pd = []
    distlist = []
        
    obj1 = astdys['Name'][i]
    try:
        #arc1 = rebound.SimulationArchive('../SBDynT/main-br-nbs/TNOs/'+str(obj1)+'/archive.bin')
        #ser1 = bin_to_df(obj1,arc1)
        ser1 = pd.read_csv('../SBDynT/main-br-nbs/TNOs/'+str(obj1)+'/series_wh.csv')
        #print(ser1)
    except:
        dist_pd.append(obj1)
        dist_pd.append(distlist)
        return dist_pd

    for j in range(i+1,len(astdys)):
        obj2 = astdys['Name'][j]
        try:
            #arc2 = rebound.SimulationArchive('../SBDynT/main-br-nbs/TNOs/'+str(obj2)+'/archive.bin')
            #ser2 = bin_to_df(obj2,arc2)
            ser2 = pd.read_csv('../SBDynT/main-br-nbs/TNOs/'+str(obj2)+'/series_wh.csv')
            #print(ser2)
        except:
            continue
        
        
        apo = ser1['a']*(1+ser1['ecc'])
        per = ser2['a']*(1-ser2['ecc'])
            
        if len(np.where(abs((apo - per)) <= 0.002)[0]) > 0:
            distlist.append(obj2)
    
    dist_pd.append(obj1)
    dist_pd.append(distlist)
    return dist_pd

if __name__ == '__main__':

    from schwimmbad import MPIPool
    #print('schwimmbad in')
    with MPIPool() as pool:
        #print('mpipool pooled')    
        if not pool.is_master():
            pool.wait()
            sys.exit(0)
            
        astdys = pd.read_csv('data_files/astdys_tnos.csv')   
        multi_per = functools.partial(per_calc, astdys=astdys)
        i = range(len(astdys))
        #i = range(1150,len(astdys))
        
        data = pool.map(multi_per, i)
        dist_df = pd.DataFrame(data,columns=['Name','dist_obj'])
        dist_df.to_csv('data_files/distlist.csv')