

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.87990ac4-a0e2-4dc2-b7c7-97e18a82764f"),
    ZCTA_by_SDoH_percentages=Input(rid="ri.foundry.main.dataset.f6ce6698-905f-445b-a7b6-4b3894d73d61"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
SELECT ZCTA_by_SDoH_percentages.*,ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZIP_CODE as zipcode
FROM ZCTA_by_SDoH_percentages JOIN ZiptoZcta_Crosswalk_2021_ziptozcta2020 on ZCTA_by_SDoH_percentages.ZCTA=ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.07977c84-4a97-4e2e-ab95-ddc4787931b2"),
    pf_sdoh=Input(rid="ri.foundry.main.dataset.7ad489be-d80b-47d5-8577-3b7d5c211112")
)
SELECT distinct city,state,zip,ZCTA
FROM pf_sdoh
where ZCTA IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cf5227e7-e5a9-4364-acce-15670de616bd"),
    LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_=Input(rid="ri.foundry.main.dataset.75d7da57-7b0e-462c-b41d-c9ef4f756198"),
    pf_sdoh=Input(rid="ri.foundry.main.dataset.7ad489be-d80b-47d5-8577-3b7d5c211112")
)
SELECT pf_sdoh.*, LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_.MDs

FROM pf_sdoh LEFT JOIN LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_ on pf_sdoh.person_id=LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7ad489be-d80b-47d5-8577-3b7d5c211112"),
    ZCTA_by_SDOH_wZIP=Input(rid="ri.foundry.main.dataset.87990ac4-a0e2-4dc2-b7c7-97e18a82764f"),
    pf_death=Input(rid="ri.foundry.main.dataset.e5cd300f-cf0b-43dc-9e96-45dfb4b01c43")
)
SELECT pf_death.zip_code, pf_death.person_id, ZCTA_by_SDOH_wZIP.*
FROM pf_death LEFT JOIN ZCTA_by_SDOH_wZIP on pf_death.zip_code=ZCTA_by_SDOH_wZIP.zipcode

