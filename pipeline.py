from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.vector.main.execute.d43bd982-c772-40e6-8a0e-1907374ab3e6")
)
"""
================================================================================
Description:
Adds hospitalization start and end dates for all hospitalization after the 
the COVID-associated hospitalization  

Index Date is defined by first_poslab_or_diagnosis_date
*** DICSUSS or ADD FLEXIBILITY ----> Global Variables??
================================================================================ 
"""
def pf_after_covid_visits():

    macrovisits_df  = (
        microvisit_to_macrovisit_lds
        .select('person_id', 'macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
    )    

    pf_has_covid_hosp_df = (
        pf_covid_visits
        .select('person_id', 'first_poslab_or_diagnosis_date', 'first_COVID_hospitalization_start_date','first_COVID_hospitalization_end_date')
        .filter(F.col('first_COVID_hospitalization_start_date').isNotNull())
    )

    pf_no_covid_hosp_df = (
        pf_covid_visits
        .select('person_id', 'first_poslab_or_diagnosis_date', 'first_COVID_hospitalization_start_date','first_COVID_hospitalization_end_date')
        .filter(F.col('first_COVID_hospitalization_start_date').isNull())
    )

    w = Window.partitionBy('person_id', 'macrovisit_id').orderBy('macrovisit_start_date')

    has_df = (
        pf_has_covid_hosp_df
        .join(macrovisits_df, 'person_id', 'inner')
        .where(F.col('first_COVID_hospitalization_start_date') < F.col('macrovisit_start_date'))
        .withColumn('macrovisit_id',            F.first('macrovisit_id').over(w))
        .withColumn('macrovisit_start_date',    F.first('macrovisit_start_date').over(w))
        .withColumn('macrovisit_end_date',      F.first('macrovisit_end_date').over(w))
        .dropDuplicates()        
    )
    print(has_df.count())

    no_df = (
        pf_no_covid_hosp_df
        .join(macrovisits_df, 'person_id', 'inner')
        .where(F.col('first_poslab_or_diagnosis_date') < F.col('macrovisit_start_date'))
        .withColumn('macrovisit_id',            F.first('macrovisit_id').over(w))
        .withColumn('macrovisit_start_date',    F.first('macrovisit_start_date').over(w))
        .withColumn('macrovisit_end_date',      F.first('macrovisit_end_date').over(w))
        .dropDuplicates()        
    )
    print(no_df.count())

    df = has_df.union(no_df)
    print(df.count())

    return df
    

