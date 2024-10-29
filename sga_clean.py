import pandas as pd

# Convert Timedelta to HH:MM:SS format, excluding NaT values
def format_timedelta(td):
    if pd.notna(td):
        hours, remainder = divmod(td.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"  

    else:
        return pd.NaT
    

def do_clean(df_data):
    df_data['tempo_total']= pd.to_datetime(df_data['dt_fim']) - pd.to_datetime(df_data['dt_cheg'])
    df_data['tempo_espera']= pd.to_datetime(df_data['dt_cha']) - pd.to_datetime(df_data['dt_cheg'])
    df_data['tempo_atendimento']= pd.to_datetime(df_data['dt_fim']) - pd.to_datetime(df_data['dt_ini'])

    df_clean = df_data.loc[((df_data.tempo_total < pd.to_timedelta("12h")) & (df_data.tempo_total >= pd.to_timedelta("0s"))) | (df_data.tempo_total.isnull())]
    df_clean['tempo_atendimento'] =  df_clean['tempo_atendimento'].apply(format_timedelta)
    df_clean['tempo_total'] =  df_clean['tempo_total'].apply(format_timedelta)
    df_clean['tempo_espera'] =  df_clean['tempo_espera'].apply(format_timedelta)
    
    return df_clean