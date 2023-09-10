def get_cnil_indexes():
    from include.sourcing.classes.get_cnil_indexes import GetCnilIndexes
    # # from include.sourcing.check_if_updated import CheckUpdateCnil
    # # from include.sourcing.get_cnil_tables_xlsx import UpdateTableCSV
    # # from include.sourcing.clean_csv_for_bq import CleanCSVForBq
    # # from include.sourcing.upload_to_bq import UploadToBq
    

    cnil_indexes = GetCnilIndexes()
    print(cnil_indexes.df_datasets[['title','last_modified']].head(5).sort_values('last_modified', ascending=False))