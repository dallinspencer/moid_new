from distlink import COrbitData, MOID_fast
from bin_to_df import bin_to_df
import rebound
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import schwimmbad
import functools
import json
import ast

def moid_calc(i, astdys):
    print(i)
    dist_pd = []
    distlist = []
        
    obj1 = astdys['Name'][i]
    try:
        #arc1 = rebound.SimulationArchive('../SBDynT/main-br-nbs/TNOs/'+str(obj1)+'/archive.bin')
        #ser1 = bin_to_df(obj1,arc1)
        ser1 = pd.read_csv('../SBDynT/main-br-nbs/TNOs/'+str(obj1)+'/series_wh.csv')
        Omega1 = (np.arcsin(ser1['p']/np.sin(ser1['inc'])))/180*np.pi
        omega1 = (np.arcsin(ser1['h']/ser1['ecc'])-Omega1)/180*np.pi
        #print(ser1)
    except:
        dist_pd.append(obj1)
        dist_pd.append(distlist)
        return dist_pd
    
    distobj = astdys['dist_obj'][i]
    distobj = ast.literal_eval(distobj)
    tracker = []
    for j in range(len(distobj)):
        obj2 = distobj[j]
        #print(distobj)
        try:
            #arc1 = rebound.SimulationArchive('../SBDynT/main-br-nbs/TNOs/'+str(obj1)+'/archive.bin')
            #ser1 = bin_to_df(obj1,arc1)
            ser2 = pd.read_csv('../SBDynT/main-br-nbs/TNOs/'+str(obj2)+'/series_wh.csv')
            #print(ser1)
        except:
            continue
        Omega2 = (np.arcsin(ser2['p']/np.sin(ser2['inc'])))/180*np.pi
        omega2 = (np.arcsin(ser2['h']/ser2['ecc'])-Omega2)/180*np.pi
        # Create two orbits (angles in radian!)
        #COrbitData is in (a,e,i,Omega,omega)
        dist = np.zeros(len(ser1))
        for k in range(len(ser1)):
            o1 = COrbitData(ser1['a'][k], ser1['ecc'][k],ser1['inc'][k]/180*np.pi,Omega1[k],omega1[k])
            o2 = COrbitData(ser2['a'][k], ser2['ecc'][k],ser2['inc'][k]/180*np.pi,Omega2[k],omega2[k])

            # Compute MOID between two orbits.
            MOID = MOID_fast(o1, o2, 2e-15, 1e-15)
            dist[k] = MOID.distance
        if len(np.where(dist <= 0.002)[0]) > 0:
            tracker.append(str(obj2))
            
    dist_pd.append(obj1)
    dist_pd.append(tracker)
    return dist_pd
        
'''
astdys = pd.read_csv('data_files/astdys_per.csv')
for i in range(len(astdys)):
    obj1 = astdys['Name'][i]
    arc1 = rebound.SimulationArchive('../SBDynT/main-br-nbs/TNOs/'+str(obj1)+'archive.bin')
    ser1 = bin_to_df(obj1,arc1)
    Omega1 = (np.arcsin(ser1['p']/np.sin(ser1['inc'])))/180*np.pi
    omega1 = (np.arcsin(ser1['h']/ser1['ecc'])-Omega1)/180*np.pi
    for j in range(i+1,len(astdys)):
        obj2 = astdys['Name'][j]
        arc2 = rebound.SimulationArchive('../SBDynT/main-br-nbs/TNOs/'+str(obj2)+'archive.bin')
        ser2 = bin_to_df(obj2,arc2)
        Omega2 = (np.arcsin(ser2['p']/np.sin(ser2['inc'])))/180*np.pi
        omega2 = (np.arcsin(ser2['h']/ser2['ecc'])-Omega2)/180*np.pi
        
        # p = np.sin(o.inc)*np.sin(o.Omega)
        # h = (o.e)*np.sin(o.Omega+o.omega)
        dist = np.zeros(len(ser1))
        # Create two orbits (angles in radian!)
        #COrbitData is in (a,e,i,Omega,omega)
        for k in range(len(ser1)):
            o1 = COrbitData(ser1['a'][k], ser1['ecc'][k],ser1['inc'][k]/180*np.pi,Omega1[k],omega1[k])
            o2 = COrbitData(ser2['a'][k], ser2['ecc'][k],ser2['inc'][k]/180*np.pi,Omega2[k],omega2[k])

            # Compute MOID between two orbits.
            MOID = MOID_fast(o1, o2, 2e-15, 1e-15)
            dist[k] = MOID.distance
        if len(np.where(dist <= 0.002)[0]) > 0:
            tracker.append(str(obj1)+'+'+str(obj2))
'''            
if __name__ == '__main__':

    from schwimmbad import MPIPool
    #print('schwimmbad in')
    with MPIPool() as pool:
        #print('mpipool pooled')    
        if not pool.is_master():
            pool.wait()
            sys.exit(0)
            
        astdys = pd.read_csv('data_files/distlist.csv')   
        multi_moid = functools.partial(moid_calc, astdys=astdys)
        i = range(len(astdys))
        #i = range(1150,len(astdys))
        
        data = pool.map(multi_moid, i)
        dist_df = pd.DataFrame(data,columns=['Name','dist_obj'])
        dist_df.to_csv('data_files/moidlist.csv')