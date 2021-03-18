
#generate df with all potential matches
def create_df_with_attributes(matches,texts):
    print("Started adding matches attributes...")
    df_matches = pd.DataFrame.from_dict(matches, orient='index')
    df_matches['matches_tuple'] = df_matches.index
    df_matches[['doc_1','doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
    df_matches = df_matches.drop(['matches_tuple'], axis=1)
    df_matches = df_matches.reset_index(drop=True)

    df_texts = pd.DataFrame(texts)
    df_texts['index'] = df_texts.index

    df_matches_full = pd.merge(df_matches, df_texts,  how='left', left_on=['doc_1'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df_texts,  how='left', left_on=['doc_2'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    return df_matches_full