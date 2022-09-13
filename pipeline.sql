

@transform_pandas(
    Output(rid="ri.vector.main.execute.c238979d-e108-496a-8523-d71d2141d08d"),
    ZCTA_by_SDoH_percentages=Input(rid="ri.foundry.main.dataset.f6ce6698-905f-445b-a7b6-4b3894d73d61"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
SELECT ZCTA_by_SDoH_percentages.*,ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZIP_CODE as zipcode
FROM ZCTA_by_SDoH_percentages JOIN ZiptoZcta_Crosswalk_2021_ziptozcta2020 on ZCTA_by_SDoH_percentages.ZCTA=ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZCTA

@transform_pandas(
    Output(rid="ri.vector.main.execute.a901890b-b30a-43e9-8e0c-647458be539a"),
    LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_=Input(rid="ri.foundry.main.dataset.75d7da57-7b0e-462c-b41d-c9ef4f756198"),
    pf_sdoh=Input(rid="ri.vector.main.execute.0aaeb9e0-f764-4186-b558-6ded1d772aca")
)
SELECT pf_sdoh.*, LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_.MDs

FROM pf_sdoh LEFT JOIN LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_ on pf_sdoh.person_id=LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.0aaeb9e0-f764-4186-b558-6ded1d772aca"),
    ZCTA_by_SDOH_wZIP=Input(rid="ri.vector.main.execute.c238979d-e108-496a-8523-d71d2141d08d"),
    pf_death=Input(rid="ri.vector.main.execute.9ecdced1-c3b3-4f1a-9b20-10bb4b1701de")
)
SELECT pf_death.zip_code, pf_death.person_id, ZCTA_by_SDOH_wZIP.*
FROM pf_death LEFT JOIN ZCTA_by_SDOH_wZIP on pf_death.zip_code=ZCTA_by_SDOH_wZIP.zipcode

