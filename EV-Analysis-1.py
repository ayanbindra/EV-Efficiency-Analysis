
################################################### '''Analysis of Data''' ###################################################

import pandas as pd
import numpy as np


'''input data'''
#nemo_df = pd.read_csv('D://Sheets/Nemo/01-2021.csv')
df_type = nemo_df[nemo_df['data_type']=="NEMO_VEHICLE_STATUS"].copy()
df_type['created_timestamp'] = df_type['created_timestamp'].astype('datetime64[ns]')
df_type['data_timestamp'] = df_type['data_timestamp'].astype('datetime64[ns]')
#df_type.drop(df_type.index[df_type['vehicle_number'] == "DL1ZX1079"], axis = 0, inplace=True)

'''Use the Below Conditions to Single Out a particular Car, Mode, etc.'''

#cond1 = df_type['vehicle_number'] == "DL1ZX1123"
#cond2 = df_type['vehicle_mode'] == "Drive"
#condall = cond1 & cond2
#df_type = df_type[cond1]

    
vehicle_data_list=[]
extracted_data_list=[]

df_type['soc_flag'] = np.nan
df_type['kmps_ch'] = np.nan
df_type['soc_change'] = np.nan
df_type['odo_change'] = np.nan
df_type['time_change'] = np.nan
df_type['vehicle_flag'] = np.nan
df_type['month'] = np.nan
df_type['date_check'] = np.nan

df_type['month'] = df_type['data_timestamp'].to_numpy().astype('datetime64[M]')
df_type['date_check'] = df_type['data_timestamp'].to_numpy().astype('datetime64[D]')


'''Analysis Loop Begins Here'''

# idx is the main iterator 
# i is outer loop to control starting of a cycle
# j is inner loop to control end of a cycle from the start point

for vehicle in df_type.vehicle_number.unique():
    print(vehicle)
    print(vehicle_data_list)
    df_vehicle = df_type.loc[df_type.vehicle_number==vehicle].copy()
    df_vehicle = df_vehicle.sort_values(by=['created_timestamp'], ascending = (True))
    
   
    x = range(0, len(df_vehicle.index))
    idx = 0
    
    
    for i in range(0, len(df_vehicle)-1):
        
        if ((idx == len(x)-1) | (idx > len(x))):
            break
    
        
        if (df_vehicle['vehicle_mode'].iloc[idx] == df_vehicle['vehicle_mode'].iloc[idx+1]):
            
            if df_vehicle['vehicle_mode'].iloc[idx] == "Drive":
                df_vehicle['soc_flag'].iloc[idx] = "Drive Start"
                
            elif df_vehicle['vehicle_mode'].iloc[idx] == "Idle":
                df_vehicle['soc_flag'].iloc[idx] = "Idle Start"
                
            elif df_vehicle['vehicle_mode'].iloc[idx] == "NC":
                df_vehicle['soc_flag'].iloc[idx] = "NC Start"
                
            elif df_vehicle['vehicle_mode'].iloc[idx] == "FC":
                df_vehicle['soc_flag'].iloc[idx] = "FC Start"
            
            soc_x = df_vehicle['soc'].iloc[idx]
            odo_x = df_vehicle['odometer'].iloc[idx]
            date_x = df_vehicle['data_timestamp'].iloc[idx]
            
            for k in range(idx+1, len(df_vehicle)-1):
                
                
                if df_vehicle['vehicle_mode'].iloc[k] != df_vehicle['vehicle_mode'].iloc[k+1]:
                    
                    if df_vehicle['vehicle_mode'].iloc[k] == "Drive":
                        df_vehicle['soc_flag'].iloc[k] = "Drive End"
                    
                    elif df_vehicle['vehicle_mode'].iloc[k] == "Idle":
                        df_vehicle['soc_flag'].iloc[k] = "Idle End"
                        
                    elif df_vehicle['vehicle_mode'].iloc[k] == "NC":
                        df_vehicle['soc_flag'].iloc[k] = "NC End"
                        
                    elif df_vehicle['vehicle_mode'].iloc[k] == "FC":
                        df_vehicle['soc_flag'].iloc[k] = "FC End"
                    
                    
                    soc_y = df_vehicle['soc'].iloc[k]
                    odo_y = df_vehicle['odometer'].iloc[k]
                    date_y = df_vehicle['data_timestamp'].iloc[k]
                    
                    diff = date_y - date_x
                    
                    
                    df_vehicle['time_change'].iloc[k] = diff.seconds/60
                    
                    df_vehicle['soc_change'].iloc[k] = abs(soc_x - soc_y)
                    df_vehicle['odo_change'].iloc[k] = abs(odo_y - odo_x)
                    df_vehicle['kmps_ch'].iloc[k] = df_vehicle['odo_change'].iloc[k] / df_vehicle['soc_change'].iloc[k]
                    
                    extracted_data_list.append(df_vehicle.iloc[k])
                    #Inserts the desired rows containing SOC and ODO change data into an extracted list
                    
                    
                    idx = idx + 1
                    break
                
                elif df_vehicle['vehicle_mode'].iloc[k] == df_vehicle['vehicle_mode'].iloc[k+1]:
                    
                    # Checks for Sudden Increase in SOC in driving or idle mode, where it should ideally be decreasing
                    if df_vehicle['soc'].iloc[k] - df_vehicle['soc'].iloc[k+1] < 0:
                        if (df_vehicle['vehicle_mode'].iloc[k] == "Drive" or df_vehicle['vehicle_mode'].iloc[k] == "Idle"):
                            df_vehicle['soc_flag'].iloc[k] = "Unexpected Increase"
                            
                            soc_y = df_vehicle['soc'].iloc[k]
                            odo_y = df_vehicle['odometer'].iloc[k]
                            date_y = df_vehicle['data_timestamp'].iloc[k]
                            
                            diff = date_y - date_x
                            
                            
                            df_vehicle['time_change'].iloc[k] = diff.seconds/60
                            
                            df_vehicle['soc_change'].iloc[k] = abs(soc_x - soc_y)
                            df_vehicle['odo_change'].iloc[k] = abs(odo_y - odo_x)
                            df_vehicle['kmps_ch'].iloc[k] = df_vehicle['odo_change'].iloc[k] / df_vehicle['soc_change'].iloc[k]
                            
                            extracted_data_list.append(df_vehicle.iloc[k])
                            
                            
                            idx = idx + 2
                            break
                        
                        else:
                            idx = idx+1
                            continue
                    
                    else:
                        
                        idx = idx + 1
                        continue
                    
        
        else:
            idx = idx+1
            continue
        
    vehicle_data_list.append(df_vehicle)

#Concatenates all the vehicles together
final_data_df = pd.concat(vehicle_data_list, ignore_index = True)


# Creating a dataframe out of Extracted List and keeping the required rows for grouping
final_extracted = pd.concat(extracted_data_list, ignore_index = True, axis = 1)
final_extracted = final_extracted.transpose()
final_extracted_sorted = final_extracted[['vehicle_number', 'created_timestamp', 'data_timestamp', 'vehicle_mode', 'kmps_ch', 'odo_change', 'soc_change', 'time_change', 'soc_flag', 'month', 'date_check']] 

# Drops all Modes Except Drive - Can be adjusted according to which modes are needed
final_extracted_sorted.drop(final_extracted_sorted.index[(final_extracted_sorted["vehicle_mode"] == "00") | (final_extracted_sorted["vehicle_mode"] == "NC") | (final_extracted_sorted["vehicle_mode"] == "FC") | (final_extracted_sorted["vehicle_mode"] == "Idle") ], axis=0, inplace=True)
final_extracted_sorted.drop(final_extracted_sorted.index[(final_extracted_sorted["kmps_ch"] == np.inf)], axis=0, inplace=True)
 
# If Month-Wise Analysis, Captures the Month of that particular dataframe 
month = final_extracted_sorted.month.iloc[0]

########################### GROUPING ####################################

'''Grouping Type - 1 : On basis of vehicle number and vehicle mode'''

#data_points = final_extracted_sorted.groupby(['vehicle_number', 'vehicle_mode']).agg({'odo_change': ['sum'], 'soc_change': ['sum']})
#data_points = data_points.reset_index()
#data_points.columns = ['vehicle_number','vehicle_mode', 'odo_change', 'soc_change']
#data_points['km_per_soc'] = data_points['odo_change']/data_points['soc_change']
#data_points['MONTH'] = month

'''Grouping Type - 2 : On basis of vehicle number'''

data_points1 = final_extracted_sorted.groupby('vehicle_number').agg({'odo_change': ['sum'], 'soc_change': ['sum'], 'time_change': ['sum'], 'vehicle_mode': ['count'], 'date_check':['nunique']})
data_points1 = data_points1.reset_index()
data_points1.columns = ['vehicle_number', 'odo_change', 'soc_change', 'total_minutes_drive', 'drive_cycles_in_month', 'days_on_road_count']
data_points1['km_per_soc'] = data_points1['odo_change']/data_points1['soc_change']
data_points1['MONTH'] = month
##############################################################

# Final List contains the appended Grouped-By Dataframes for the vehicles. To be initialised only once.
#final_list = []


#Append grouped by Dataframe into Final List
final_list.append(data_points1)

#After Analysis of all months is complete, concatenate Final List to form Final Dataframe
final_data = pd.concat(final_list, ignore_index=True)



''' Only the input data and grouping constraints should be changed. The code in the middle needs no changes'''
