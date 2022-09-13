from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.vector.main.execute.a517adaf-9a91-4b6c-ab22-28a537e81890"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def macrovisit_multi_ip(microvisit_to_macrovisit_lds):

    df = (
        microvisit_to_macrovisit_lds
        .select(
            'person_id',
            'macrovisit_id', 
            'visit_occurrence_id', 
            'visit_concept_id',
            'visit_concept_name', 
            'visit_start_date', 
            'visit_end_date',
            'macrovisit_start_date',
            'macrovisit_end_date'
        )
        .filter(F.col('macrovisit_id') == "4659206354756305685_1_969748783")
        .sort('visit_concept_id')
    )

    return df
    

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
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.9224b962-92a4-41c5-8c3b-8c726c2356e5")
)
"""
================================================================================
Description:
Adds hospitalization start and end dates and optionally Emergency Room visit 
dates. (To get both sets of dates, set get_er_and_hosp_visits == True)  
================================================================================ 
"""
def pf_covid_visits_dep():

    """
    ================================================================================
    Potential Parameters 
    --------------------
    get_er_and_hosp_visits (boolean)
    False - gets only hospitalizations
    True  - gets hospitalizations and emergency room visits 

    requires_lab_and_diagnosis (boolean)
    True  - first poslab date AND diag date are used to associate with visits
    False - first poslab date OR diag date are used to associate with visits

    num_days_before / num_days_after (int)
    Proximity in days between index date(s) and visit date
    *** NEEDS DISCUSSION ***  
    ================================================================================ 
    """
    get_er_and_hosp_visits      = True    
    requires_lab_and_diagnosis  = False
    num_days_before             = 1
    num_days_after              = 16

    pf_df = pf_comorbidities

    # Reduce patient columns and create column with the number of 
    # of days between the poslab and diagnosis dates
    pf1_df = (
        pf_df
            .select('person_id', 'first_pos_pcr_antigen_date', 'first_pos_diagnosis_date', 'first_poslab_or_diagnosis_date')
            .withColumn('poslab_minus_diag_date', F.datediff('first_pos_pcr_antigen_date', 'first_pos_diagnosis_date'))
    )

    # Reduce microvisit_to_macrovisit_lds columns and joined to contain patients
    pf_visits_df = (
        microvisit_to_macrovisit_lds
            .select('person_id','visit_start_date','visit_concept_id', 'macrovisit_id', 'macrovisit_start_date','macrovisit_end_date')
            .join(pf1_df,'person_id','inner')  
    )

    """
    ================================================================================
    Get list of Emergency Room Visit concept_set_name values from our spreadsheet 
    and use to create a list of associated concept_id values  
    ================================================================================
    """
    er_concept_names = list(
        our_concept_sets
            .filter(our_concept_sets.er_only_visit == 1)
            .select('concept_set_name').toPandas()['concept_set_name']
    )    
    er_concept_ids = (
        list(concept_set_members
                .where(( concept_set_members.concept_set_name.isin(er_concept_names)) & 
                       ( concept_set_members.is_most_recent_version == 'true'))
                .select('concept_id').toPandas()['concept_id']
        )
    )
    print(er_concept_ids)

    """
    ================================================================================ 
    Get Emergency Room visits (null macrovisit_start_date values and is in 
    er_concept_ids) and
    create the following columns: 
    poslab_minus_ER_date      - used for hospitalizations that require *both*
                                poslab and diagnosis
    first_index_minus_ER_date - used for hospitalizations that require *either*
                                poslab or diagnosis
    ================================================================================                                
    """
    er_df = (
        pf_visits_df
            .where(pf_visits_df.macrovisit_start_date.isNull() & (pf_visits_df.visit_concept_id.isin(er_concept_ids)))
            .withColumn('poslab_minus_ER_date', 
                F.datediff('first_pos_pcr_antigen_date',     'visit_start_date'))
            .withColumn("first_index_minus_ER_date", 
                F.datediff("first_poslab_or_diagnosis_date", "visit_start_date"))         
    )

    """
    ================================================================================

    GET ER VISITS    

    ================================================================================
    """
    if requires_lab_and_diagnosis == True:
        er_df = (
            er_df
                .withColumn("poslab_associated_ER", 
                             F.when(F.col('poslab_minus_ER_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .withColumn("poslab_and_diag_associated_ER", 
                             F.when((F.col('poslab_associated_ER') == 1) & 
                                    (F.col('poslab_minus_diag_date').between(-num_days_after, num_days_before)), 1).otherwise(0)
                )
                .where(F.col('poslab_and_diag_associated_ER') == 1)
                .withColumnRenamed('visit_start_date', 'covid_ER_only_start_date')
                .select('person_id', 'covid_ER_only_start_date')
                .dropDuplicates()
        )     
    else:
        er_df = (
            er_df
                .withColumn("poslab_or_diag_associated_ER", 
                             F.when(F.col('first_index_minus_ER_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .where(F.col('poslab_or_diag_associated_ER') == 1)
                .withColumnRenamed('visit_start_date', 'covid_ER_only_start_date')                             
                .select('person_id', 'covid_ER_only_start_date')
                .dropDuplicates()
        )        

    # get first er visit within range of Covid index date
    first_er_df = (
        er_df
        .groupBy('person_id')
        .agg(F.min('covid_ER_only_start_date').alias('first_covid_er_only_start_date'))    
    )

    """ 
    ================================================================================    
    Get Hospitalization visits (non-null macrovisit_start_date values) and
    create the following columns: 
    poslab_minus_hosp_date      - used for hospitalizations that require *both*
                                  poslab and diagnosis
    first_index_minus_hosp_date - used for hospitalizations that require *either*
                                  poslab or diagnosis
    ================================================================================                                  
    """
    all_hosp_df = (
        pf_visits_df
            .where(pf_visits_df.macrovisit_start_date.isNotNull())
            .withColumn("poslab_minus_hosp_date", 
                F.datediff("first_pos_pcr_antigen_date",     "macrovisit_start_date"))
            .withColumn("first_index_minus_hosp_date", 
                F.datediff("first_poslab_or_diagnosis_date", "macrovisit_start_date"))    
    )

    """
    ================================================================================
    To have a hospitalization associated with *both* Positive PCR/Antigen test  
    and Covid Diagnosis, the test and diagnosis date need to be close together
    and the test and hospitalization must be close together. 
    
    Specifically:
    1. The hosp date must be within [num_days_before, num_days_after] of the poslab date 
       AND
    2. The diag date must be within [num_days_before, num_days_after] of the poslab date

    Example:
    ==============================================================
    [num_days_before, num_days_after] = [1,16]
    poslab date     = June 10 [June 9, June 16]
    diag date       = June 12 
    hosp date       = June 22
    
    1. Hospitalization must occur between June 9 and June 26: true
    2. Diagnosis date  must occur between June 9 and June 26: true
    ==============================================================

    Otherwise, to get hospitalizations associated with *either* a positive 
    PCR/Antigen test *or* a positive Covid Diagnosis, the first index date
    (whichever comes first:  PCR/Antigen or Diagnosis date) and the 
    hospitalization date must be close together. 
    ================================================================================    
    """
    if requires_lab_and_diagnosis == True:
        hosp_df = (
            all_hosp_df
                .withColumn("poslab_associated_hosp", 
                             F.when(F.col('poslab_minus_hosp_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .withColumn("poslab_and_diag_associated_hosp", 
                             F.when( (F.col('poslab_associated_hosp') == 1) & 
                                     (F.col('poslab_minus_diag_date').between(-num_days_after, num_days_before)), 1).otherwise(0)
                )
                .where(F.col('poslab_and_diag_associated_hosp') == 1)
                .withColumnRenamed('macrovisit_start_date', 'covid_hospitalization_start_date')
                .withColumnRenamed('macrovisit_end_date',   'covid_hospitalization_end_date')
                .select('person_id', 'macrovisit_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date')
                .dropDuplicates()
        )     
    else:
        hosp_df = (
            all_hosp_df
                .withColumn("poslab_or_diag_associated_hosp", 
                             F.when(F.col('first_index_minus_hosp_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .where(F.col('poslab_or_diag_associated_hosp') == 1)
                .withColumnRenamed('macrovisit_start_date', 'covid_hospitalization_start_date')
                .withColumnRenamed('macrovisit_end_date',   'covid_hospitalization_end_date')          
                .select('person_id', 'macrovisit_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date')
                .dropDuplicates()
        )
    
    # get first hospitalization period within Covid index date range 
    w = Window.partitionBy('person_id').orderBy('covid_hospitalization_start_date')

    first_hosp_df = (
        hosp_df
        .withColumn('macrovisit_id', F.first('macrovisit_id').over(w))
        .withColumn('first_COVID_hospitalization_start_date', F.first('covid_hospitalization_start_date').over(w))
        .withColumn('first_COVID_hospitalization_end_date',   F.first('covid_hospitalization_end_date').over(w))
        .select('person_id', 'macrovisit_id', 'first_COVID_hospitalization_start_date', 'first_COVID_hospitalization_end_date')
        .dropDuplicates()
    )

    """
    ================================================================================
    If get_er_and_hosp_visits == True, include ER and hospital visits, otherwise 
       only include hospital visits.
    ================================================================================    
    """
    if get_er_and_hosp_visits == True:
        first_visits_df = first_hosp_df.join(first_er_df, 'person_id', 'outer')
    else:
        first_visits_df = first_hosp_df
         
             
    # Join in person facts
    pf_first_visits_df = pf_df.join(first_visits_df, 'person_id', 'left')    
  

  
    return pf_first_visits_df

@transform_pandas(
    Output(rid="ri.vector.main.execute.86a7ebcc-e90b-4a39-a7d1-8ec1b179e61e"),
    ALL_COVID_POS_PATIENTS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:

Input:

Output:
================================================================================
"""
def pf_sample(ALL_COVID_POS_PATIENTS):

    proportion_of_patients_to_use = 1.

    return ALL_COVID_POS_PATIENTS.sample(False, proportion_of_patients_to_use, 111)
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.c2a030cf-5f1d-47df-a3f2-97ff57139951"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def successive_macrovisits(microvisit_to_macrovisit_lds):

    df = (
        microvisit_to_macrovisit_lds
        .select(
            'person_id',
            'macrovisit_id', 
            'visit_occurrence_id', 
            'visit_concept_id',
            'visit_concept_name', 
            'visit_start_date', 
            'visit_end_date',
            'macrovisit_start_date',
            'macrovisit_end_date'
        )
        .filter(F.col('macrovisit_id').isNotNull())
        .filter(F.col('person_id') == "5506278855900148501")
    ).sort('macrovisit_start_date')

    return df
    

