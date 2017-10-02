# global import statements
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser

def read_xer(file_name):
    ''' Opens an Primavera P6 XER file and returns a dictionary of entity names and data frames containing values.'''
    
    import io
    # Reading XER file
    with io.open(file_name, "r", encoding="ISO-8859-1") as f:
        lstLines = list(f)

    # Getting List of Tables
    lstTables = [(x, lstLines.index(x)) for x in lstLines if x.find('%T') != -1]
    lstTables.append(("%E",len(lstLines) - 1))

    dict = {}
    
    for tbl in lstTables:
        if tbl[0] != "%E":
            i = [lstTables.index(x) for x in lstTables if x[0] == tbl[0]][0]
            h = lstLines[lstTables[i][1] + 1].split('\t')[1:]
            
            lstAsText = lstLines[lstTables[i][1] + 2:lstTables[i + 1][1]]
            lst = [x.split('\t')[1:] for x in lstAsText]
            
            df = pd.DataFrame.from_records(data = lst, columns = h)

            dict["".join(tbl[0].split()[1])] = df

    return dict, pd.to_datetime(dict['PROJECT']['last_recalc_date'])
   
    
def time_phase_monthly(df, id_field_name, start_field_name, finish_field_name):
    ''' Applies time-phasing operation on a data frame.'''
    
    df.loc[:,start_field_name] = pd.to_datetime(df.loc[:,start_field_name])
    df.loc[:,finish_field_name] = pd.to_datetime(df.loc[:,finish_field_name])

    lstTimePhased = []

    for index, row in df.iterrows():
        if (pd.isnull(row[start_field_name]) == True | pd.isnull(row[finish_field_name]) == True):
            print row[id_field_name], ' has blank Start or Finish date(s).'
            continue
            
        ASD = row[start_field_name]
        AFD = row[finish_field_name]
        
        StartPointer = datetime.datetime(year = ASD.year, month = ASD.month, day = 1, hour = 0, minute = 0, second = 0)   
        FinishPointer = datetime.datetime(year = AFD.year, month = AFD.month , day = 1, hour = 0, minute = 0, second = 0) + relativedelta(months = 1)

        ISD = StartPointer
        IFD = StartPointer + relativedelta(months = 1)

        while True:        
            if (min(IFD, AFD) - max(ISD,ASD)).total_seconds > 0.0:
                lstTimePhased.append([index, str(ISD.year) + "-" + str(ISD.month).rjust(2, '0'), (min(IFD, AFD) - max(ISD, ASD)).total_seconds()/(3600 * 24)])
                ISD = IFD
                IFD = IFD + relativedelta(months = 1)
            else:
                print row[id_field_name], ' has zero duration.'
                break

            if IFD > FinishPointer: break

    dfTimePhased = pd.DataFrame.from_records(columns = [id_field_name,'interval','duration'], data = lstTimePhased)
    df = pd.concat([df,dfTimePhased.pivot(index=id_field_name, columns = 'interval', values='duration')], axis = 1, join='inner')   
    
    return df
	
def find_children(return_obj, lookup_value, df, parent_column_name, id_column_name):
    ''' Finds all children in a hierarchical (self-related) data frame.'''
    for idx, item in df[df[parent_column_name] == lookup_value].iterrows():
        return_obj.append(item[id_column_name])
        find_children(return_obj, item[id_column_name], df, parent_column_name, id_column_name)

def get_hierarchical_paths(return_list, lookup_value, parent_short_name, df, parent_column_name, id_column_name, field_name_to_aggregate):
    ''' calculates all paths in a hierarchical (self-related) data frame.'''
    for idx, item in df[df[parent_column_name] == lookup_value].iterrows():
        return_list.append({'id':item[id_column_name], 'path':parent_short_name + '.' + item[field_name_to_aggregate]})
        get_hierarchical_paths(return_list, item[id_column_name], parent_short_name + '.' + item[field_name_to_aggregate], df, parent_column_name, id_column_name, field_name_to_aggregate)
        
def fill_date_range_gaps(df):
    rng = pd.period_range(df.index.min(), df.index.max(), freq='M')
    df_ranges = pd.DataFrame(data=rng.to_series().astype(np.string_).values, index = rng, columns=['period_range'])
    df_return = df_ranges.merge(df, left_on='period_range', right_index=True, how='left')
    del df_return['period_range']
    return df_return

def get_tasks_by_wbs_paths(lst_wbs_inclusions, lst_wbs_exclusions, df_xer):
    df_wbs = df_xer['PROJWBS']
    
    # Separating Project Nodes from WBS Nodes
    lst_project_node_wbs_ids = df_wbs[df_wbs['proj_node_flag'] == 'Y']['wbs_id']
    #df_wbs_nodes = df_wbs[~df_wbs['wbs_id'].isin(lst_project_node_wbs_ids)]

    df_wbs_temp = pd.DataFrame()
    for proj in lst_project_node_wbs_ids:
        # Adding path to wbs data frame
        lst_wbs_path_project=[]
        get_hierarchical_paths(lst_wbs_path_project, proj, proj, df_wbs, 'parent_wbs_id', 'wbs_id', 'wbs_short_name')
        df_wbs_temp = df_wbs_temp.append(pd.DataFrame(lst_wbs_path_project))

    df_wbs = df_wbs.merge(df_wbs_temp, left_on='wbs_id', right_on='id', how='left')
    df_wbs['path'] = df_wbs['path'].str[len(proj) + 1:]
    del df_wbs['id']
    
    # Filtering wbs includions
    df_wbs_included = df_wbs[df_wbs['path'].isin(lst_wbs_inclusions)]
    
    lst_wbs_id_included_all = []
    # Finding all WBS items under filtered WBSs
    for wbs_id in df_wbs_included['wbs_id']:
        lst_wbs_ids=[]
        find_children(lst_wbs_ids, wbs_id, df_wbs, 'parent_wbs_id', 'wbs_id')
        lst_wbs_id_included_all.extend(lst_wbs_ids)
    
    # Adding includions themselves
    lst_wbs_id_included_all.extend(df_wbs[df_wbs['path'].isin(lst_wbs_inclusions)]['wbs_id'])
    
    # Filtering wbs items for all children
    #df_wbs_included_all = df_wbs[(df_wbs['wbs_id'].isin(lst_wbs_id_included_all))]
    
    # Filtering Exclusions
    df_wbs_excluded = df_wbs[df_wbs['path'].isin(lst_wbs_exclusions)]
    
    lst_wbs_id_excluded_all = []
    # Finding all WBS items under filtered WBSs
    for wbs_id in df_wbs_excluded['wbs_id']:
        lst_wbs_ids=[]
        find_children(lst_wbs_ids, wbs_id, df_wbs, 'parent_wbs_id', 'wbs_id')
        lst_wbs_id_excluded_all.extend(lst_wbs_ids)
    
    # Adding excludions themselves
    lst_wbs_id_excluded_all.extend(df_wbs[df_wbs['path'].isin(lst_wbs_exclusions)]['wbs_id'])
    
    # Filtering wbs items for all children and project nodes
    #df_wbs_excluded_all = df_wbs[(df_wbs['wbs_id'].isin(lst_wbs_id_excluded_all))]
    
    # Finding tasks under all wbs items
    df_tasks = df_xer['TASK']
    df_tasks = df_tasks[df_tasks['wbs_id'].isin(set(lst_wbs_id_included_all) - set(lst_wbs_id_excluded_all))]
    
    return df_tasks
	
	
	
	
def create_added_removed_data_frame(lst_ids_1, lst_ids_2):
    df = pd.DataFrame(index=set(lst_ids_1).union(set(lst_ids_2)))
    df.ix[df.index.isin(lst_ids_1) & df.index.isin(lst_ids_2), 'status'] = 'normal'
    df.ix[df.index.isin(lst_ids_1) & ~df.index.isin(lst_ids_2), 'status'] = 'removed'
    df.ix[~df.index.isin(lst_ids_1) & df.index.isin(lst_ids_2), 'status'] = 'added'
    return df
	
def get_task_activity_code_assignments(df_xer, activity_code_type_name):
    # Reading Activity Code Types
    df_activity_code_types = df_xer['ACTVTYPE'][['actv_code_type_id', 'actv_code_type']]

    act_code_type_id = df_activity_code_types[df_activity_code_types['actv_code_type'] == activity_code_type_name]['actv_code_type_id'].values[0]
        
    # Reading Activity Codes
    df_activity_codes = df_xer['ACTVCODE'][df_xer['ACTVCODE']['actv_code_type_id'] == act_code_type_id][['actv_code_id', 'actv_code_name', 'short_name', 'parent_actv_code_id']]
    
    lst_activity_code_ids = df_activity_codes[df_activity_codes['parent_actv_code_id'] == '']['actv_code_id']
    df_paths = pd.DataFrame()

    # Adding Activity Code path in hierarchy
    for act_code_id in lst_activity_code_ids:
        lst_activity_code_path = []
        get_hierarchical_paths(lst_activity_code_path, act_code_id, df_activity_codes[df_activity_codes['actv_code_id'] == act_code_id]['short_name'].values[0], df_activity_codes, 'parent_actv_code_id', 'actv_code_id', 'short_name')
        df_paths = df_paths.append(pd.DataFrame(lst_activity_code_path))

    # Merging if there are hierarchies
    if df_paths.shape[0] > 0:
        df_activity_codes = df_activity_codes.merge(df_paths, left_on='actv_code_id', right_on='id', how='left')
        
    # Setting parent level paths
    df_activity_codes.ix[df_activity_codes['parent_actv_code_id'] == '', 'path'] =  df_activity_codes.ix[df_activity_codes['parent_actv_code_id'] == '', 'short_name']

    # Deleting extra keys
    del df_activity_codes['parent_actv_code_id']
    if 'id' in df_activity_codes.columns:
        del df_activity_codes['id']

    # Reading Activity Code Assignments to activities
    df_activity_code_assignments = df_xer['TASKACTV'][df_xer['TASKACTV']['actv_code_type_id'] == act_code_type_id][['task_id', 'actv_code_id']]

    # Merging all
    df_all = df_activity_code_assignments.merge(df_activity_codes, how='left', left_on = 'actv_code_id', right_on = 'actv_code_id')
    
    return df_all

def adjust_p6_actualized_early_late_dates(df_schedule):
    '''
    Adjusts early and late dates for actualized activities based on planned start planned finish, actual start and finish and late start and finish dates.
    '''
    df = df_schedule.copy()
    # Converting date fields from object to dates
    df['early_start_date'] = df['early_start_date'].values.astype(np.datetime64)
    df['early_end_date'] = df['early_end_date'].values.astype(np.datetime64)
    df['late_start_date'] = df['late_start_date'].values.astype(np.datetime64)
    df['late_end_date'] = df['late_end_date'].values.astype(np.datetime64)
    df['target_start_date'] = df['target_start_date'].values.astype(np.datetime64)
    df['target_end_date'] = df['target_end_date'].values.astype(np.datetime64)
    
    # Not Started Activities
    # All remain the same
    df.loc[df['status_code'] == 'TK_NotStart', 'early_start_date_adjusted'] = df.loc[df['status_code'] == 'TK_NotStart', 'early_start_date']
    df.loc[df['status_code'] == 'TK_NotStart', 'early_end_date_adjusted'] = df.loc[df['status_code'] == 'TK_NotStart', 'early_end_date']
    df.loc[df['status_code'] == 'TK_NotStart', 'late_start_date_adjusted'] = df.loc[df['status_code'] == 'TK_NotStart', 'late_start_date']
    df.loc[df['status_code'] == 'TK_NotStart', 'late_end_date_adjusted'] = df.loc[df['status_code'] == 'TK_NotStart', 'late_end_date']
    
    # Completed and In-Progress Activities:
    # 1- Changing early start date to planned start date
    df.loc[df['status_code'] != 'TK_NotStart', 'early_start_date_adjusted'] = df.loc[df['status_code'] != 'TK_NotStart', 'target_start_date'] 
    # 2- Changing early finish date to planned finish date
    df.loc[df['status_code'] != 'TK_NotStart', 'early_end_date_adjusted'] = df.loc[df['status_code'] != 'TK_NotStart', 'target_end_date']
    # 3- Changing late start date to late end date - planned duration
    df.loc[df['status_code'] != 'TK_NotStart', 'late_start_date_adjusted'] = df.loc[df['status_code'] != 'TK_NotStart', 'late_end_date'] - df.loc[df['status_code'] != 'TK_NotStart', 'target_end_date'] + df.loc[df['status_code'] != 'TK_NotStart', 'target_start_date']
    df.loc[df['status_code'] != 'TK_NotStart', 'late_end_date_adjusted'] = df.loc[df['status_code'] != 'TK_NotStart', 'late_end_date'] 
    
    return df

def polarize_update_schedule(df, data_date, early_start_field_name, early_finish_field_name
                             , late_start_field_name, late_finish_field_name
                            , actual_start_field_name, actual_finish_field_name):
    '''Divides an update schedule to update Actualized with actual dates and update planned with early and late dates'''
    
    # Filtering for Actualized Activities
    df_update_actual = df[df['status_code'] != 'TK_NotStart'].copy()
    
    # Letting Actual Starts remain
    df_update_actual.loc[:, 'act_start_date_adjusted'] = pd.to_datetime(df_update_actual.loc[:, actual_start_field_name])
    
    # Setting data date as actual finish date for in-progress activities
    if df_update_actual.loc[df_update_actual['status_code'] == 'TK_Active'].shape[0] !=0:
        df_update_actual.loc[df_update_actual['status_code'] == 'TK_Active', 'act_end_date_adjusted'] = data_date
    
    # Setting actual finish date as actual finish date for completed activities
    if df_update_actual.loc[df_update_actual['status_code'] == 'TK_Complete'].shape[0] !=0:
        df_update_actual.loc[df_update_actual['status_code'] == 'TK_Complete', 'act_end_date_adjusted'] = pd.to_datetime(df_update_actual.loc[df_update_actual['status_code'] == 'TK_Complete', actual_finish_field_name])
    
    # Filtering for Update Plan Activities
    df_update_plan = df[df['status_code'] != 'TK_Complete'].copy()
    
    # Letting Early and Late dates be Remaining Early and Late for In-Progress and non started activities
    df_update_plan.loc[:, 'early_start_date_adjusted'] = pd.to_datetime(df_update_plan.loc[:, early_start_field_name])
    df_update_plan.loc[:, 'early_end_date_adjusted'] = pd.to_datetime(df_update_plan.loc[:, early_finish_field_name])
    df_update_plan.loc[:, 'late_start_date_adjusted'] = pd.to_datetime(df_update_plan.loc[:, late_start_field_name])
    df_update_plan.loc[:, 'late_end_date_adjusted'] = pd.to_datetime(df_update_plan.loc[:, late_finish_field_name])
    
    return df_update_plan, df_update_actual


def calculate_earned_duration(df_baseline_tasks, df_update_tasks,
                              id_field_name, baseline_early_start_field_name, baseline_early_finish_field_name,
                              baseline_late_start_field_name, baseline_late_finish_field_name,
                              update_early_start_field_name, update_early_finish_field_name,
                              update_late_start_field_name, update_late_finish_field_name,
                              update_actual_start_field_name, update_actual_finish_field_name,
                              fix_zero_actual_duration_flag, update_data_date):

    num_baseline_columns = len(df_baseline_tasks.columns)
    num_update_columns = len(df_update_tasks.columns)
    
    # Calculating Baseline Duration (Early_Finish - Early_Start)
    df_baseline_tasks['baseline_duration'] = pd.to_datetime(df_baseline_tasks[baseline_early_finish_field_name]) - pd.to_datetime(df_baseline_tasks[baseline_early_start_field_name])
    
    # TODO: Fixing update activities with actual dates in the future
    #df_update_tasks.ix[df_update_tasks['act_start_date'] > update_data_date, '']
    
    # Finding Added/Removed Activities
    df_removed = df_baseline_tasks[~(df_baseline_tasks[id_field_name].isin(df_update_tasks[id_field_name]))]
    df_added = df_update_tasks[~(df_update_tasks[id_field_name].isin(df_baseline_tasks[id_field_name]))]
    
    # Finding matched activities
    df_baseline_matched = df_baseline_tasks[df_baseline_tasks[id_field_name].isin(df_update_tasks[id_field_name])]
    df_update_matched = df_update_tasks[df_update_tasks[id_field_name].isin(df_baseline_tasks[id_field_name])]

    # Adding baseline_duration to update_matched
    df_update_matched = df_update_matched.merge(df_baseline_matched[[id_field_name, 'baseline_duration']], left_on= id_field_name, right_on = id_field_name, how = 'inner') 
    
    # Fixing activities with 0 At Completion Duration
    if (fix_zero_actual_duration_flag == True):
        # Completed Activities
        for i, row in df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] != ''), :].iterrows():
            if parser.parse(row[update_actual_finish_field_name]) == parser.parse(row[update_actual_start_field_name]):
                df_update_matched.loc[i, update_actual_start_field_name] = parser.parse(df_update_matched.loc[i, update_actual_finish_field_name]) - df_update_matched.loc[i, 'baseline_duration']
                
    # Calculating At-Completion Duration for update_matched
    #   Completed Activities (Actual_Finish - Actual_Start)
    df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] != ''), 'at_completion_duration'] = pd.to_datetime(df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] != ''), update_actual_finish_field_name]) - pd.to_datetime(df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] != ''), update_actual_start_field_name])
    #   In-Progress Activities (Early_Finish - Actual_Start)
    df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] == ''), 'at_completion_duration'] = pd.to_datetime(df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] == ''), update_early_finish_field_name]) - pd.to_datetime(df_update_matched.loc[(df_update_matched[update_actual_start_field_name] != '') & (df_update_matched[update_actual_finish_field_name] == ''), update_actual_start_field_name]) 
    #   Not-Started Activities (Early_Finish - Early_Start)
    df_update_matched.loc[(df_update_matched[update_actual_start_field_name] == '') & (df_update_matched[update_actual_finish_field_name] == ''), 'at_completion_duration'] = pd.to_datetime(df_update_matched.loc[(df_update_matched[update_actual_start_field_name] == '') & (df_update_matched[update_actual_finish_field_name] == ''), update_early_finish_field_name]) - pd.to_datetime(df_update_matched.loc[(df_update_matched[update_actual_start_field_name] == '') & (df_update_matched[update_actual_finish_field_name] == ''), update_early_start_field_name])
    
    # Polarizing Update File
    df_update_matched_plan, df_update_matched_actual = polarize_update_schedule(df_update_matched, update_data_date, update_early_start_field_name, update_early_finish_field_name, update_late_start_field_name, update_late_finish_field_name, update_actual_start_field_name, update_actual_finish_field_name)
    
    # Time Phasing Matched
    #    Calculating Baseline Time-Phased
    df_baseline_matched_early_time_phased = time_phase_monthly(df_baseline_matched, id_field_name, baseline_early_start_field_name, baseline_early_finish_field_name)
    df_baseline_matched_late_time_phased = time_phase_monthly(df_baseline_matched, id_field_name, baseline_late_start_field_name, baseline_late_finish_field_name)
    #    Calculating Update Time-Phased
    df_update_matched_actual_time_phased = time_phase_monthly(df_update_matched_actual, id_field_name, 'act_start_date_adjusted', 'act_end_date_adjusted')
    df_update_matched_plan_early_time_phased = time_phase_monthly(df_update_matched_plan, id_field_name, 'early_start_date_adjusted', 'early_end_date_adjusted')
    df_update_matched_plan_late_time_phased = time_phase_monthly(df_update_matched_plan, id_field_name, 'late_start_date_adjusted', 'late_end_date_adjusted')
    
    # Time Phasing Removed
    if df_removed.shape[0] !=0:
        df_removed_time_phased = time_phase_monthly(df_removed, id_field_name, baseline_early_start_field_name, baseline_early_finish_field_name)
    else:
        print 'There are no removed activities.'
        df_removed_time_phased = None
    
    # Time Phasing Added
    if df_added.shape[0] !=0:
        df_added_plan, df_added_actual = polarize_update_schedule(df_added, update_data_date, update_early_start_field_name, update_early_finish_field_name, update_late_start_field_name, update_late_finish_field_name, update_actual_start_field_name, update_actual_finish_field_name)
        
        df_added_plan_time_phased = pd.DataFrame()
        df_added_actual_time_phased = pd.DataFrame()

        if df_added_plan.shape[0] !=0:
            df_added_plan_time_phased = time_phase_monthly(df_added_plan, id_field_name, 'early_start_date_adjusted', 'early_end_date_adjusted' )
        if df_added_actual.shape[0] !=0:
            df_added_actual_time_phased = time_phase_monthly(df_added_actual, id_field_name, 'act_start_date_adjusted', 'act_end_date_adjusted' )

    else:
        print 'There are no added activities.'
        df_added_plan_time_phased = None
        df_added_actual_time_phased = None
    
    # Calculate earned duration for matched Actual
    df_earned_matched_time_phased = df_update_matched_actual_time_phased.copy() 
    df_earned_matched_time_phased = df_earned_matched_time_phased[(df_earned_matched_time_phased['baseline_duration'] != datetime.timedelta(0.0)) & (df_earned_matched_time_phased['at_completion_duration'] != datetime.timedelta(0.0))]
    #df_earned_matched_time_phased.ix[:,61+4:] = df_earned_matched_time_phased.ix[:,61+4:].mul(df_earned_matched_time_phased.apply(lambda row: row['baseline_duration'] / row['at_completion_duration'], axis =1), axis = 0)
    print num_baseline_columns, num_update_columns
    df_earned_matched_time_phased.ix[:,num_update_columns + 4:] = df_earned_matched_time_phased.ix[:,num_update_columns+4:].mul(df_earned_matched_time_phased.apply(lambda row: row['baseline_duration'] / row['at_completion_duration'], axis =1), axis = 0)
    return df_baseline_matched_early_time_phased, df_baseline_matched_late_time_phased, df_update_matched_actual_time_phased, df_update_matched_plan_early_time_phased, df_update_matched_plan_late_time_phased, df_removed_time_phased, df_added_plan_time_phased, df_added_actual_time_phased, df_earned_matched_time_phased
	