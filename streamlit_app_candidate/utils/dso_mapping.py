def generate_dso_mapping(employer, dso_mapping):
    employer_list = [x.lower() for x in dso_mapping["org_name"].tolist()]
    if employer.lower() in employer_list:
        return dso_mapping.loc[dso_mapping['org_name'] == employer, 'tag'].iloc[0]
    else:
        return "Undefined"