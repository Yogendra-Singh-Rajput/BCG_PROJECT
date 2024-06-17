# Import SparkSession
from pyspark.sql import SparkSession, Window
from datetime import datetime as dt
import os
from config import *
from pyspark.sql.functions import dense_rank, desc, col, sum, count
from pyspark.sql.types import StringType, IntegerType

# Create a New Spark Session
spark = SparkSession.builder.appName('BCG_Analytic_Project').getOrCreate()

class Data_Analytics:
    '''
        Class Name Defined as Data_Analytics.
        Class has defined initiation method __init__, it is being used to read all the requierd data frames.        
        This Class has multiple functions(Callable) objects defined. 
        Functions are listed below with their significance - 
        
    '''
    
    def __init__(self, INPUT_FOLDER):
        self.df_dict = {}
        for i in os.listdir(INPUT_FOLDER):
            if i[-4:] == '.csv':
                self.df_dict[f'df_{i[:-4]}'.lower()] = spark.read.csv(f'{INPUT_FOLDER}/{i}', header=True)

    def Analytics1(self):
        '''
            Count of cases(accidents) where number of male deaths is greater than 2.
        '''
        df = self.df_dict['df_primary_person_use']
        df = df.where((df.PRSN_GNDR_ID == "MALE") & (df.DEATH_CNT == 1)).groupBy('CRASH_ID').count().where("count>2")
        cn = df.count()
        if cn > 0:
            df.show()
        return f'Count of cases(accidents) where number of male deaths is greater than 2 = {cn}'
    
    def Analytics2(self):
        '''
            Number of Two Wheelers booked for Crashes.
        '''
        df = self.df_dict['df_units_use']
        df = df.where(df.VEH_BODY_STYL_ID.isin(['MOTORCYCLE','POLICE MOTORCYCLE']))
        df.show()
        return f'Number of Two Wheelers booked for Crashes = {df.count()}'
    
    def Analytics3(self):
        '''
            Top 5 Vehicle Makers of the cars present in the crashes in which driver died and Airbags did not deploy
        '''
        df1 = self.df_dict['df_primary_person_use']
        df2 = self.df_dict['df_units_use']
        x1=df1.alias('df1').join(df2.alias('df2'),(df1.CRASH_ID == df2.CRASH_ID) & (df1.UNIT_NBR==df2.UNIT_NBR), 'left').where((df1['DEATH_CNT']==1) & (df1['PRSN_AIRBAG_ID']=='NOT DEPLOYED')).groupBy(['VEH_MAKE_ID','df1.CRASH_ID','df1.UNIT_NBR']).count().groupBy('VEH_MAKE_ID').count().orderBy('count',ascending=False).limit(5)
        x2=df1.alias('df1').join(df2.alias('df2'),(df1.CRASH_ID == df2.CRASH_ID) & (df1.UNIT_NBR==df2.UNIT_NBR), 'left').where((df1['DEATH_CNT']==1) & (df1['PRSN_AIRBAG_ID']=='NOT DEPLOYED') & (df2['VEH_MAKE_ID']!='NA')).groupBy(['VEH_MAKE_ID','df1.CRASH_ID','df1.UNIT_NBR']).count().groupBy('VEH_MAKE_ID').count().orderBy('count',ascending=False).limit(5)
        return f'''
Top 5 Vehicle Makers of the cars present in the crashes in which driver died and Airbags did not deploy

If NA in Data is Allowed:
{x1._jdf.showString(20,20,False)}
ANSWER = {x1.select('VEH_MAKE_ID').rdd.flatMap(lambda x:x).collect()}

If NA in Data is Not Allowed:
{x2._jdf.showString(20,20,False)}
ANSWER = {x2.select('VEH_MAKE_ID').rdd.flatMap(lambda x:x).collect()}
'''
    
    def Analytics4(self):
        '''
            Number of Vehicles with driver having valid licences involved in hit and run
        '''
        df1 = self.df_dict['df_primary_person_use'] # valid license 
        #df2 = self.df_dict['df_charges_use']
        df3 = self.df_dict['df_units_use'] # Hit & Run
        
        #cn = df1.alias('df1').join(df2.alias('df2'),df1.CRASH_ID==df2.CRASH_ID,'left').join(df3.alias('df3'),df1.CRASH_ID == df3.CRASH_ID,'left').select(['df1.CRASH_ID','DRVR_LIC_TYPE_ID','CHARGE','df3.VIN']).where((~df1.DRVR_LIC_TYPE_ID.isin(['UNKNOWN','NA','UNLICENSED'])) & (df2.CHARGE.rlike('.*HIT AND RUN.*')) & (df1.PRSN_TYPE_ID=='DRIVER') & (~df1.DRVR_LIC_CLS_ID.isin(['UNKNOWN','NA','UNLICENSED']))).groupBy(['CRASH_ID','VIN']).count().count()
        cn =  df1.alias('df1').join(df3.alias('df3'),(df1.CRASH_ID==df3.CRASH_ID) & (df1.UNIT_NBR==df3.UNIT_NBR),'left').where((~df1.DRVR_LIC_TYPE_ID.isin(['UNKNOWN','NA','UNLICENSED'])) & (~df1.DRVR_LIC_CLS_ID.isin(['UNKNOWN','NA','UNLICENSED'])) & (df1.PRSN_TYPE_ID == 'DRIVER') & (df3.VEH_HNR_FL=='Y')).groupBy(['df1.CRASH_ID','df3.VIN']).count().groupby('df3.VIN').count().count()
        return f'Number of Vehicles with driver having valid licences involved in hit and run = {cn}'
    
    def Analytics5(self):
        '''
            State which has highest number of accidents in which females are not involved.
        '''
        df1 = self.df_dict['df_primary_person_use']
        df1 = df1.where('PRSN_GNDR_ID=="MALE"').groupBy(['DRVR_LIC_STATE_ID','CRASH_ID']).count().groupBy('DRVR_LIC_STATE_ID').count().orderBy('count',ascending=False).limit(1)
        res = df1.select('DRVR_LIC_STATE_ID').collect()[0].DRVR_LIC_STATE_ID
        return f'''
{df1._jdf.showString(20,20,False)}

State which has highest number of accidents in which females are not involved - ANSWER = {res}
'''
    
    def Analytics6(self):
        '''
            Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        '''
        df1 = self.df_dict['df_units_use']
        #df1=df1.withColumn('TOTAL',df1.TOT_INJRY_CNT+df1.DEATH_CNT).select(['VEH_MAKE_ID','TOTAL']).groupBy('VEH_MAKE_ID').sum('TOTAL').orderBy('sum(TOTAL)',ascending=False).withColumn('drank',dense_rank().over(Window.orderBy(desc('sum(TOTAL)'))))                                            
        df1 = df1.where('DEATH_CNT > 0').groupBy('VEH_MAKE_ID').agg(sum('TOT_INJRY_CNT').cast(IntegerType()).alias('TIC'))
        df1 = df1.orderBy('TIC',ascending=False).withColumn('drank',dense_rank().over(Window.orderBy(desc('TIC'))))
        df1 = df1.where((df1.drank>=3)&(df1.drank<=5))

        return f'''
{df1._jdf.showString(20,20,False)}

Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death - Answer = {df1.select('VEH_MAKE_ID').rdd.flatMap(lambda x:x).collect()}
'''

    def Analytics7(self):
        '''
            For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        '''
        df1 = self.df_dict['df_primary_person_use']
        df2 = self.df_dict['df_units_use']
        df = df1.alias('df1').join(df2.alias('df2'),(df1.CRASH_ID==df2.CRASH_ID) & (df1.UNIT_NBR==df2.UNIT_NBR),'left').groupBy(['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID']).count().withColumn('drank',dense_rank().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('count')))).where('drank==1')
        return f'''
The top ethnic user group of each unique body style - 

{df._jdf.showString(30,30,False)}
'''

    def Analytics8(self):
        '''
            Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        '''
        df1 = self.df_dict['df_primary_person_use']
        df2 = self.df_dict['df_units_use']
        x1 = df1.alias('df1').join(df2.alias('df2'),(df1.CRASH_ID==df2.CRASH_ID) & (df1.UNIT_NBR==df2.UNIT_NBR),'left').select(['df1.CRASH_ID','DRVR_ZIP','CONTRIB_FACTR_1_ID','CONTRIB_FACTR_2_ID','CONTRIB_FACTR_P1_ID']).where((df2.CONTRIB_FACTR_1_ID.rlike('.*ALCOHOL.*')) | (df2.CONTRIB_FACTR_2_ID.rlike('.*ALCOHOL.*')) | (df2.CONTRIB_FACTR_P1_ID.rlike('.*ALCOHOL.*'))).groupBy(['DRVR_ZIP','CRASH_ID']).count().groupBy('DRVR_ZIP').count().orderBy('count',ascending=False).limit(5)
        x2 = df1.alias('df1').join(df2.alias('df2'),(df1.CRASH_ID==df2.CRASH_ID) & (df1.UNIT_NBR==df2.UNIT_NBR),'left').select(['df1.CRASH_ID','DRVR_ZIP','CONTRIB_FACTR_1_ID','CONTRIB_FACTR_2_ID','CONTRIB_FACTR_P1_ID']).where((df2.CONTRIB_FACTR_1_ID.rlike('.*ALCOHOL.*')) | (df2.CONTRIB_FACTR_2_ID.rlike('.*ALCOHOL.*')) | (df2.CONTRIB_FACTR_P1_ID.rlike('.*ALCOHOL.*'))).groupBy(['DRVR_ZIP','CRASH_ID']).count().where(~df1.DRVR_ZIP.isNull()).groupBy('DRVR_ZIP').count().orderBy('count',ascending=False).limit(5)
        return f'''
Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

If Null is Allowed

{x1._jdf.showString(20,20,False)}

Top 5 Zip Codes with Highest Crashes - Answer = {[i.DRVR_ZIP for i in x1.select('DRVR_ZIP').collect()]}

If Null is Not Allowed

{x2._jdf.showString(20,20,False)}

Top 5 Zip Codes with Highest Crashes - Answer = {[i.DRVR_ZIP for i in x2.select('DRVR_ZIP').collect()]}
'''

    def Analytics9(self):
        '''
            Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        '''
        df1 = self.df_dict['df_units_use']
        df2 = self.df_dict['df_damages_use']
        df1 = df1.withColumn('SCL_1',df1.VEH_DMAG_SCL_1_ID.substr(9,1)).withColumn('SCL_2',df1.VEH_DMAG_SCL_2_ID.substr(9,1))    
        cn = df1.alias('df1').join(df2.alias('df2'),df1.CRASH_ID==df2.CRASH_ID,'left').select(['df1.CRASH_ID','DAMAGED_PROPERTY','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID','SCL_1','SCL_2','FIN_RESP_TYPE_ID']).where((df2.DAMAGED_PROPERTY.isNull()) & ((df1.SCL_1>4) | (df1.SCL_2>4)) & (~df1.FIN_RESP_TYPE_ID.isin(['NA']))).groupBy('CRASH_ID').count().count()
        
        return f'''
Count of Distinct Crash IDs where No Damaged Property was observed 
and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance - Count = {cn}
'''

    def Analytics10(self):
        '''
            Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
            has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
            with highest number of offences (to be deduced from the data)
        '''
        df1 = self.df_dict['df_units_use']
        df2 = self.df_dict['df_charges_use']
        df3 = self.df_dict['df_primary_person_use']
        
        top_25_states = df1.where(~df1.VEH_LIC_STATE_ID.isNull()).groupBy(['VEH_LIC_STATE_ID','CRASH_ID']).count().groupBy('VEH_LIC_STATE_ID').count().orderBy('count',ascending=False).select('VEH_LIC_STATE_ID').limit(25)
        top_25_states = [i.VEH_LIC_STATE_ID for i in top_25_states.collect()]
        top_10_used_vehicle_colours = df1.where(~df1.VEH_COLOR_ID.isin('NA')).groupBy('VEH_COLOR_ID').count().orderBy('count',ascending=False).limit(10)
        top_10_used_vehicle_colours = [i.VEH_COLOR_ID for i in top_10_used_vehicle_colours.collect()]
        df = df3.alias('df3').join(df1.alias('df1'),(df3.CRASH_ID==df1.CRASH_ID) & (df3.UNIT_NBR==df1.UNIT_NBR),'left').join(df2.alias('df2'), (df3.CRASH_ID==df2.CRASH_ID) & (df3.UNIT_NBR==df2.UNIT_NBR) & (df3.PRSN_NBR==df2.PRSN_NBR), 'left').where((df2.CHARGE.rlike('.*SPEED.*')) & (~df3.DRVR_LIC_CLS_ID.isin(['NA','UNKNOW','UNLICENSED'])) & (df3.PRSN_TYPE_ID=='DRIVER') & (df1.VEH_COLOR_ID.isin(top_10_used_vehicle_colours)) & (df1.VEH_LIC_STATE_ID.isin(top_25_states))).groupBy('df3.CRASH_ID','df3.UNIT_NBR','df1.VEH_MAKE_ID').count().groupBy('VEH_MAKE_ID').count().orderBy('count',ascending=False).limit(5)

        return f'''
Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
with highest number of offences (to be deduced from the data)

{df._jdf.showString(20,20,False)}

ANSWER = {[i.VEH_MAKE_ID for i in df.select('VEH_MAKE_ID').collect()]}
'''



# Defined the Class obect, also ran the class initiation function
analytics_object = Data_Analytics(INPUT_FOLDER)

# Final Result will be saved in this empty string, that will be written in output file.
result_string = ""

# To save the Multiple Analytic Results into the string.
for i in range(1,11):
    try:
        Analytics_res = eval(f'analytics_object.Analytics{i}()')
        result_string += f'********************************************************************************************\n\nAnalytics {i}\n\n{Analytics_res}\n\n'
        print('\n********************************************************************************************\n')
        print(Analytics_res)
    except Exception as e:
        result_string += str(e) + '\n\n'

result_string += '********************************************************************************************\n\n'

m = 0
if OUTPUT_FILE_VERSIONING:
    l = [int(i[12:i.index('.')]) for i in os.listdir(OUTPUT_FOLDER)]
    m = max(l) + 1

# writing string data to the output file
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
with open(f'{OUTPUT_FOLDER}/Result_File_{m}.txt','w') as file:
    file.write(result_string)
    file.write(dt.now().strftime('Date = %d-%B-%Y, %A') + '\n')
    file.write(dt.now().strftime('Time = %H:%M:%S'))