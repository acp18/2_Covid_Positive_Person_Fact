

@transform_pandas(
    Output(rid="ri.vector.main.execute.c238979d-e108-496a-8523-d71d2141d08d"),
    ZCTA_by_SDoH_percentages=Input(rid="ri.foundry.main.dataset.f6ce6698-905f-445b-a7b6-4b3894d73d61"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
SELECT ZCTA_by_SDoH_percentages.*,ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZIP_CODE as zipcode
FROM ZCTA_by_SDoH_percentages JOIN ZiptoZcta_Crosswalk_2021_ziptozcta2020 on ZCTA_by_SDoH_percentages.ZCTA=ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZCTA

