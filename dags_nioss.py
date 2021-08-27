# coding: utf-8

import os

import numpy as np

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.postgres_hook import PostgresHook

from common.configs import RELATION_PATH
from common.db import QueryExecutor
from common.mixins import CellTypeMixin, FrequencyMixin
from common.processor import pd

from queries.nioss import NIOSS_RAW_QUERY, RG_MAP_QUERY, \
    RSR_PLAN_QUERY, RSR_SHARING_PLAN, RSR_MOD_SWAP_PLAN, SHARING_INFO


class EnrichTopology(FrequencyMixin, CellTypeMixin):
    region_code = {
        'Амурская область': 28,
        'Астраханская область': 90,
        'Алтайский край': 22,
        'Волгоградская область': 34,
        'Вологодская область': 35,
        'Еврейская АО': 79,
        'Забайкальский край': 75,
        'Иркутская область': 38,
        'Кировская область': 43,
        'Красноярский край': 24,
        'Мурманская область': 51,
        'Омская область': 55,
        'Пермский край': 59,
        'Псковская область': 60,
        'Республика Марий Эл': 12,
        'Хабаровский край': 27,
        'Ханты-Мансийский АО': 86,
        'Челябинская область': 74,
        'Чукотский АО': 87,
        'Нижегородская область': 52,
        'Республика Удмуртия': 18,
        'Тверская область': 69,
        'Ивановская область': 37,
        'Калужская область': 40,
        'Ярославская область': 76,
        'Республика Дагестан': '05',
        'Республика Северная Осетия (Алания)': 15,
        'Томская область': 70,
        'Кабардино-Балкарская Республика': '07',
        'Калининградская область': 39,
        'Камчатский край': 41,
        'Карачаево-Черкесская Республика': '09',
        'Кемеровская область': 42,
        'Краснодарский край и Адыгея': 23,
        'Краснодарский край и Республика Адыгея': 23,
        'Курганская область': 45,
        'Магаданская область': 49,
        'Новгородская область': 53,
        'Оренбургская область': 56,
        'Орловская область': 57,
        'Пензенская область': 58,
        'Приморский край': 25,
        'Республика Алтай': '04',
        'Республика Бурятия': '03',
        'Республика Башкортостан': '02',
        'Республика Ингушетия': '06',
        'Республика Калмыкия': '08',
        'Республика Кабардино-Балкария': '07',
        'Республика Карачаево-Черкессия': '09',
        'Республика Мордовия': 13,
        'Республика Саха (Якутия)': 14,
        'Республика Татарстан': 16,
        'Республика Тыва': 17,
        'Республика Хакасия	': 19,
        'Республика Чувашия': 21,
        'С.-Петербург и Ленинградская область': 78,
        'Ростовская область': 61,
        'Саратовская область': 64,
        'Свердловская область': 66,
        'Самарская область': 63,
        'Сахалинская область': 65,
        'Ставропольский край': 26,
        'Тульская область': 71,
        'Тюменская область': 78,
        'Ульяновская область': 73,
        'Чеченская Республика': 95,

        'Республика Карелия': 10,
        'Архангельская область': 29,
        'Белгородская область': 31,
        'Брянская область': 32,
        'Владимирская область': 33,
        'Воронежская область': 36,
        'Костромская область': 44,
        'Курская область': 46,
        'Липецкая область': 48,
        'Москва': 77,
        'Ненецкий АО': 83,
        'Новосибирская область': 54,
        'Республика Коми': 11,
        'Рязанская область': 62,
        'Санкт-Петербург и Ленинградская область': 78,
        'Смоленская область': 67,
        'Тамбовская область': 68,
        'Ямало-Ненецкий АО': 89
    }

    def __call__(self):
        topology = self._get_topology_data()
        cpao_map = self._get_cpao_map()
        rsr_data = self._get_rsr_data()
        rsr_mod_swap = self._get_rsr_mod_and_swap_data()
        rsr_sharing = self._get_rsr_sharing_data()

        xls = pd.ExcelFile('/opt/services/socrat_import_module/build/dags/resources/sharing_info_vc.xlsx')
        df_vc_actual = self._compare_vc_actual_data(xls, cpao_map)

        topology = topology.astype({'SHR_GRAD': float, 'DLG_GRAD': float})
        nioss_pl_records = topology.groupby('PL').agg({
            'SHR_GRAD': 'mean',
            'DLG_GRAD': 'mean'
        }).reset_index()
        nioss_pl_map = {
            x['PL']: {'SHR_GRAD': x['SHR_GRAD'], 'DLG_GRAD': x['DLG_GRAD']}
            for x in nioss_pl_records.to_dict('records')
        }

        upd_rsr_construction = self.compare_rsr_construction_sharing(rsr_data, rsr_sharing)

        rsr_plan = pd.concat([upd_rsr_construction, rsr_mod_swap], ignore_index=True)

        def mean_coords_val(series):
            count = 0
            avr_coord = 0
            coords_values = series.to_dict().values()
            for coord in coords_values:
                error_value = False
                if coord and not np.isnan(np.array(coord, dtype=np.float64)):
                    try:
                        avr_coord += float(coord)
                    except:
                        error_value = True
                    if not error_value:
                        count += 1
            return avr_coord / count if count != 0 else ''

        lat_lon_plan = rsr_plan.groupby('PL').agg({
            'SHR_GRAD': mean_coords_val,
            'DLG_GRAD': mean_coords_val
        }).reset_index()
        pl_lat_lon_plan = {
            x['PL']: {'SHR_GRAD': x['SHR_GRAD'], 'DLG_GRAD': x['DLG_GRAD']}
            for x in lat_lon_plan.to_dict('records')
        }

        rsr_plan['SHR_GRAD'] = rsr_plan['PL'].map(
            lambda x: nioss_pl_map[x]['SHR_GRAD'] if nioss_pl_map.get(x) else pl_lat_lon_plan[x]['SHR_GRAD']
        )
        rsr_plan['DLG_GRAD'] = rsr_plan['PL'].map(
            lambda x: nioss_pl_map[x]['DLG_GRAD'] if nioss_pl_map.get(x) else pl_lat_lon_plan[x]['DLG_GRAD']
        )

        vc_pl_records = df_vc_actual.groupby('PL').agg({
            'SHR_GRAD': mean_coords_val,
            'DLG_GRAD': mean_coords_val
        }).reset_index()
        vc_pl_map = {
            x['PL']: {'SHR_GRAD': x['SHR_GRAD'], 'DLG_GRAD': x['DLG_GRAD']}
            for x in vc_pl_records.to_dict('records')
        }
        df_vc_actual['SHR_GRAD'] = df_vc_actual['PL'].map(
            lambda x: nioss_pl_map[x]['SHR_GRAD'] if nioss_pl_map.get(x) else vc_pl_map[x]['SHR_GRAD']
        )
        df_vc_actual['DLG_GRAD'] = df_vc_actual['PL'].map(
            lambda x: nioss_pl_map[x]['DLG_GRAD'] if nioss_pl_map.get(x) else vc_pl_map[x]['DLG_GRAD']
        )

        topology['CELL_TYPE'] = topology['REG'].map(lambda x: self.CELL_TYPE.get(x, 'N/A'))
        topology['CELL_NAME1'] = topology.apply(self.__prepare_cells, axis=1)
        topology['BTS'] = topology.apply(self.__prepare_bts, axis=1)
        topology['CONTR'] = topology.apply(self.__prepare_controller, axis=1)
        topology['CELL_STATUS'] = topology.apply(self.__get_cell_status, axis=1)

        topology = self.compare_on_air_and_plan(topology, rsr_plan)
        topology['CELL_STATUS'] = topology.apply(self.__get_cell_status, axis=1)

        regions_processor = self._get_regions_processor(cpao_map)

        topology = pd.concat([topology, df_vc_actual], ignore_index=True)

        topology['CPIO'] = topology['PL'].map(regions_processor)
        topology['ADR'] = topology['ADR'].map(lambda x: '"{}"'.format(x) if x else '')
        topology = topology[topology['CPIO'] != False]

        self._export(topology)

    def compare_rsr_construction_sharing(self, rsr_data, rsr_sharing):
        ranges = rsr_sharing.groupby('PL').agg({
            'STND': self.groupe_ranges
        }).reset_index()
        rsr_sharing_map = {x['PL']: x['STND'] for x in ranges.to_dict('records')}

        upd_rsr_construction = rsr_data.copy()
        upd_rsr_construction.drop(upd_rsr_construction.index, inplace=True)

        for pl, pl_group in rsr_data.groupby('PL'):
            for stnd, stnd_group in pl_group.groupby('STND'):
                if pl in rsr_sharing_map:
                    if stnd not in rsr_sharing_map[pl]:
                        upd_rsr_construction = upd_rsr_construction.append(stnd_group)
                else:
                    upd_rsr_construction = upd_rsr_construction.append(stnd_group)

        return pd.concat([upd_rsr_construction, rsr_sharing], ignore_index=True)

    def compare_on_air_and_plan(self, topology, rsr_plan):
        topology_without_dismantling = topology[
            topology['CELL_STATUS'].str.contains('Эфир') | topology['CELL_STATUS'].str.contains('План')
        ]
        topology_plan = topology[topology['CELL_STATUS'] == 'План']
        topology_other = topology[topology['CELL_STATUS'] != 'План']

        ranges = topology_without_dismantling.groupby('PL').agg({
            'STND': self.groupe_ranges
        }).reset_index()
        topology_on_air_map = {x['PL']: x['STND'] for x in ranges.to_dict('records')}

        ranges = rsr_plan.groupby('PL').agg({
            'STND': self.groupe_ranges
        }).reset_index()
        rsr_plan_map = {x['PL']: x['STND'] for x in ranges.to_dict('records')}

        # Отсеиваем планы, которые есть в MTSBTS, но нет в ДКРИС
        upd_topology_plan = topology_plan.copy()
        upd_topology_plan.drop(upd_topology_plan.index, inplace=True)

        for pl, pl_group in topology_plan.groupby('PL'):
            for stnd, stnd_group in pl_group.groupby('STND'):
                if pl in rsr_plan_map:
                    if stnd in rsr_plan_map[pl]:
                        upd_topology_plan = upd_topology_plan.append(stnd_group)

        # Проверяем, что диапазона из планов нет в эфире
        upd_rsr_plan = rsr_plan.copy()
        upd_rsr_plan.drop(upd_rsr_plan.index, inplace=True)

        for pl, pl_group in rsr_plan.groupby('PL'):
            for stnd, stnd_group in pl_group.groupby('STND'):
                if pl in topology_on_air_map:
                    if stnd not in topology_on_air_map[pl]:
                        upd_rsr_plan = upd_rsr_plan.append(stnd_group)
                else:
                    upd_rsr_plan = upd_rsr_plan.append(stnd_group)

        # Объединяем отфитрованные DataFrame'ы
        topology_plan = pd.concat([upd_topology_plan, upd_rsr_plan], ignore_index=True)
        ranges = topology_plan.groupby('PL').agg({
            'STND': self.join_by_slash
        }).reset_index()
        ranges = {x['PL']: x['STND'] for x in ranges.to_dict('records')}
        topology_plan['TARGET_RANGES'] = topology_plan['PL'].map(lambda x: ranges.get(x, ''))

        return pd.concat([topology_other, topology_plan], ignore_index=True)

    @staticmethod
    def _export(data):
        print('Preparing data for export...')

        data['row_id'] = range(len(data))

        print('Replacing \\n characters...')
        data['ZDN'] = data['ZDN'].str.strip()

        print('Exporting...')
        path = os.path.join(RELATION_PATH, 'topology.csv')
        data.to_csv(path, index=False, encoding='cp1251')

    def _get_topology_data(self):
        topology_hook = OracleHook(oracle_conn_id='nioss')
        topology_executor = QueryExecutor(topology_hook)

        t_columns, t_data = topology_executor.get_records(NIOSS_RAW_QUERY)
        topology = pd.DataFrame(t_data, columns=t_columns)

        topology = self._unify_frequencies(topology, 'STND')

        return topology

    @staticmethod
    def _get_cpao_map():
        regions_hook = MsSqlHook(mssql_conn_id='atlas')
        regions_executor = QueryExecutor(regions_hook)

        r_columns, r_data = regions_executor.get_records(RG_MAP_QUERY)
        r_header_map = {k: i for i, k in enumerate(r_columns)}
        cpao_map = {row[r_header_map['REG_CODE']]: row[r_header_map['CPAO']] for row in r_data}

        return cpao_map

    @staticmethod
    def _get_regions_processor(cpao_map):
        def processor(value):
            try:
                splitted_value = value.split('_')
                reg_id = int(splitted_value[1])
            except Exception:
                reg_id = None
            return cpao_map.get(reg_id, False)

        return processor

    def _get_rsr_sharing_data(self):
        pg_hook = PostgresHook(postgres_conn_id='dkris')
        executor = QueryExecutor(pg_hook)

        columns, data = executor.get_records(RSR_SHARING_PLAN)
        data = pd.DataFrame(data, columns=columns)

        return self.__process_rsr_sharing_data(data)

    def __process_rsr_sharing_data(self, data):
        gf = data[['cellidnioss', 'address', 'oper_ved', 'cellid_operator',
                   'range', 'latitude', 'longitude', 'status_bs', 'event_bs',
                   'owner_infra', 'address_program']]
        gf = self._unify_frequencies(gf, 'range')
        gf = pd.concat([gf.assign(az=i) for i in (0, 120, 240)])

        gf = gf.rename(columns={
            'cellidnioss': 'PL',
            'address': 'ADR',
            'latitude': 'SHR_GRAD',
            'longitude': 'DLG_GRAD',
            'range': 'STND',
            'az': 'AZ1',
            'oper_ved': 'BTS_OWNER',
            'cellid_operator': 'PRTNR_SITE_NMBR',
            'status_bs': 'ADM_STATUS_BTS',
            'event_bs': 'SHARING_BS_EVENT',
            'owner_infra': 'SH_INFRSTR_OWNR',
            'address_program': 'AP_AFFILIATION'
        })

        gf = gf[~(gf['BTS_OWNER'].str.contains('МТС'.encode('utf-8')) &
                  (gf['SHARING_BS_EVENT'].isin(['присоединение ведомого'.encode('utf-8')])))]

        gf['CELL_NAME1'] = 'N/A'
        gf['UNINSTALL_DATE'] = ''
        gf['VVEDENO'] = ''

        return gf

    def _get_rsr_sharing_data_temp(self):
        pg_hook = PostgresHook(postgres_conn_id='dkris')
        executor = QueryExecutor(pg_hook)

        columns, data = executor.get_records(RSR_SHARING_PLAN)
        data = pd.DataFrame(data, columns=columns)

        return self.__process_rsr_sharing_data_temp(data)

    def __process_rsr_sharing_data_temp(self, data):
        gf = data[['cellidnioss', 'address', 'range', 'latitude', 'longitude']]
        gf = self._unify_frequencies(gf, 'range')
        gf = pd.concat([gf.assign(az=i) for i in (0, 120, 240)])

        gf = gf.rename(columns={
            'cellidnioss': 'PL',
            'address': 'ADR',
            'latitude': 'SHR_GRAD',
            'longitude': 'DLG_GRAD',
            'range': 'STND',
            'az': 'AZ1'
        })

        gf['CELL_NAME1'] = 'N/A'
        gf['UNINSTALL_DATE'] = ''
        gf['VVEDENO'] = ''

        return gf

    def _get_rsr_mod_and_swap_data(self):
        pg_hook = PostgresHook(postgres_conn_id='dkris')
        executor = QueryExecutor(pg_hook)

        columns, data = executor.get_records(RSR_MOD_SWAP_PLAN)
        data = pd.DataFrame(data, columns=columns)

        return self._process_rsr_swap_mod_data(data)

    def _process_rsr_swap_mod_data(self, data):
        gf = data[['cellidnioss', 'address', 'range']]
        gf = self._unify_frequencies(gf, 'range')
        gf = pd.concat([gf.assign(az=i) for i in (0, 120, 240)])

        gf = gf.rename(columns={
            'cellidnioss': 'PL',
            'address': 'ADR',
            'range': 'STND',
            'az': 'AZ1'
        })

        gf['IS_SWAP'] = 'Yes'
        gf['CELL_NAME1'] = 'N/A'
        gf['UNINSTALL_DATE'] = ''
        gf['VVEDENO'] = ''

        return gf

    def _get_rsr_data(self):
        pg_hook = PostgresHook(postgres_conn_id='dkris')
        executor = QueryExecutor(pg_hook)

        columns, data = executor.get_records(RSR_PLAN_QUERY)
        data = pd.DataFrame(data, columns=columns)

        return self.__process_rsr_data(data)

    def __process_rsr_data(self, data):
        type_map = {
            'OD': 'Outdoor',
            'ID': 'Indoor',
            'ID/OD': 'Indoor / Outdoor'
        }

        gf = data[['cellidnioss', 'address', 'type', 'ams', 'range', 'latitude', 'longitude']]
        gf['type'] = gf['type'].map(lambda x: type_map.get(x, 'N/A'))

        gf = self._unify_frequencies(gf, 'range')

        ranges = gf.groupby('cellidnioss').agg({
            'range': self.join_by_slash
        }).reset_index()
        ranges = {x['cellidnioss']: x['range'] for x in ranges.to_dict('records')}

        gf['TARGET_RANGES'] = gf['cellidnioss'].map(lambda x: ranges.get(x, ''))
        gf = pd.concat([gf.assign(az=i) for i in (0, 120, 240)])

        gf = gf.rename(columns={
            'cellidnioss': 'PL',
            'address': 'ADR',
            'ams': 'T_BASHN',
            'latitude': 'SHR_GRAD',
            'longitude': 'DLG_GRAD',
            'range': 'STND',
            'az': 'AZ1',
            'type': 'CELL_TYPE'
        })
        gf['CELL_NAME1'] = 'N/A'
        gf['UNINSTALL_DATE'] = ''
        gf['VVEDENO'] = ''

        return gf

    def _compare_vc_actual_data(self, xls, cpao_map):
        def only_float_value(value):
            try:
                value = float(value)
            except:
                value = None
            return value

        cell_type = {
            'outdoor': 'Outdoor',
            'indoor': 'Indoor'
        }

        cell_status = {
            'Действующая': 'Эфир',
            'Плановая': 'План'
        }

        columns = ['PL', 'ADR', 'SHR_GRAD', 'DLG_GRAD', 'STND', 'AZ1', 'CELL_NAME1', 'ANT1', 'OBL_RUS',
                   'EL_NK1', 'H1_M', 'NK1', 'CELL_TYPE', 'CELL_STATUS', 'ADM_STATUS_BTS', 'TARGET_RANGES',
                   'PRTNR_SITE_NMBR', 'BTS_OWNER', 'SH_INFRSTR_OWNR', 'H_BASHN', 'T_BASHN']

        df_vc_actual = pd.read_excel(xls, 'Format 2 - for planner')
        df_vc_actual = df_vc_actual.rename(columns={
            'MTS site number': 'PL',
            'Site address (installation address eNodeB)': 'ADR',
            'Subject RF': 'OBL_RUS',
            'Latitude': 'SHR_GRAD',
            'Longitude': 'DLG_GRAD',
            'STND LTE': 'STND',
            'AZI': 'AZ1',
            'VC cell': 'CELL_NAME1',
            'Antena': 'ANT1',
            'Altitude': 'H1_M',
            'Electric tilt': 'EL_NK1',
            'Mechanical tilt': 'M_NK',
            'Status еNodeB': 'ADM_STATUS_BTS',
            'VC site number': 'PRTNR_SITE_NMBR'
        })

        df_vc_actual = df_vc_actual[df_vc_actual['ADM_STATUS_BTS'] != '#Н/Д']
        df_vc_actual = df_vc_actual[df_vc_actual.PRTNR_SITE_NMBR.notnull()]

        # df_2g3g_actual = pd.read_excel(xls, 'Format 3 - vedo')
        # df_2g3g_actual = df_2g3g_actual.rename(columns={
        #     'VC site number': 'PRTNR_SITE_NMBR',
        #     'Subject RF': 'REG',
        #     'Site address (installation address eNodeB)': 'ADR',
        #     'Latitude': 'SHR_GRAD',
        #     'Longitude': 'DLG_GRAD',
        #     'Altitude': 'H_BASHN'
        # })

        topology_hook = OracleHook(oracle_conn_id='nioss')
        topology_executor = QueryExecutor(topology_hook)

        t_columns, t_data = topology_executor.get_records(SHARING_INFO)
        sharing_mv_28a = pd.DataFrame(t_data, columns=t_columns)

        sharing_plan_df = self._get_rsr_sharing_data()

        # df_2g3g_actual['PRTNR_SITE_NMBR'] = df_2g3g_actual['PRTNR_SITE_NMBR'].astype(str)
        # df_2g3g_actual['ADM_STATUS_BTS'] = 'Действующая'

        # df_vc_actual = pd.concat([df_vc_actual, df_2g3g_actual], ignore_index=True)

        # df_vc_actual = sharing_mv_28a.merge(
        #     df_vc_actual,
        #     on=['REG', 'PRTNR_SITE_NMBR', 'STND'],
        #     how = 'right',
        #     indicator = True
        # ).query('_merge == "right_only"').drop('_merge', 1)

        sharing_vc_list = []
        sharing_vc_map = {}

        new_vc_df = df_vc_actual.copy()
        df_exist_pl = df_vc_actual.copy()

        new_vc_df.drop(new_vc_df.index, inplace=True)
        df_exist_pl.drop(df_exist_pl.index, inplace=True)

        for region_name, r_group in sharing_mv_28a.groupby('OBL_RUS'):
            for vc_name, vc_group in r_group.groupby('PRTNR_SITE_NMBR'):
                for stnd, stnd_group in vc_group.groupby('STND'):
                    vc_key = '{}_{}'.format(region_name, vc_name)
                    vc_key_stnd = '{}_{}_{}'.format(region_name, vc_name, stnd)
                    if vc_key_stnd not in sharing_vc_list:
                        sharing_vc_list.append(vc_key_stnd)
                        sharing_vc_map[vc_key] = stnd_group.to_dict('records')[0]['SITE_NAME']

        for vc_name, vc_group in sharing_plan_df.groupby('PRTNR_SITE_NMBR'):
            for stnd, stnd_group in vc_group.groupby('STND'):
                for row in stnd_group.to_dict('records'):
                    pl_region_code = row['PL'].split('_')[1]
                    for region, region_cd in self.region_code.items():
                        if str(region_cd) == str(pl_region_code):
                            vc_key = '{}_{}'.format(region, vc_name)
                            vc_key_stnd = '{}_{}_{}'.format(region, vc_name, stnd)
                            if vc_key_stnd not in sharing_vc_list:
                                sharing_vc_list.append(vc_key_stnd)
                                sharing_vc_map[vc_key] = row['PL']

        for region_name, r_group in df_vc_actual.groupby('OBL_RUS'):
            for vc_name, vc_group in r_group.groupby('PRTNR_SITE_NMBR'):
                for stnd, stnd_group in vc_group.groupby('STND'):
                    vc_key = '{}_{}'.format(region_name, vc_name)
                    vc_key_stnd = '{}_{}_{}'.format(region_name, vc_name, stnd)
                    if vc_key_stnd not in sharing_vc_list:
                        exist = False
                        for vc_28a_key in sharing_vc_list:
                            vc_key_in_map = '{}_{}'.format(vc_28a_key.split('_')[0], vc_28a_key.split('_')[1])
                            if vc_key_in_map == vc_key and vc_28a_key != vc_key:
                                exist = True
                                stnd_group['PL'] = sharing_vc_map[vc_key]
                                df_exist_pl = df_exist_pl.append(stnd_group)
                        if not exist:
                            new_vc_df = new_vc_df.append(stnd_group)

        new_vc_df['PL'] = new_vc_df.apply(
            lambda x: 'VC_{}_{}'.format(
                self.region_code.get(x['OBL_RUS']),
                x['PRTNR_SITE_NMBR']
            ), axis=1
        )

        ranges = new_vc_df.groupby('PL').agg({
            'STND': self.join_by_slash
        }).reset_index()
        ranges = {x['PL']: x['STND'] for x in ranges.to_dict('records')}
        new_vc_df['TARGET_RANGES'] = new_vc_df['PL'].map(lambda x: ranges.get(x, ''))

        new_vc_df = pd.concat([new_vc_df, df_exist_pl], ignore_index=True)

        new_vc_df['CELL_TYPE'] = new_vc_df['outdoor/indoor'].map(lambda x: cell_type.get(x, 'Outdoor'))

        for i, row in new_vc_df.iterrows():
            try:
                el_tilt = int(row['EL_NK1'])
            except:
                el_tilt = 0

            try:
                mech_tilt = int(row['M_NK'])
            except:
                mech_tilt = 0

            new_vc_df.loc[i, 'NK1'] = el_tilt + mech_tilt

        new_vc_df['NK1'] = new_vc_df['NK1'].astype(int)
        new_vc_df['CELL_STATUS'] = new_vc_df['ADM_STATUS_BTS'].map(lambda x: cell_status.get(x, 'Эфир'))
        new_vc_df['H_BASHN'] = new_vc_df['H_BASHN'].map(only_float_value)
        new_vc_df['BTS_OWNER'] = 'ВК'
        new_vc_df['SH_INFRSTR_OWNR'] = 'Инфраструктура только для партнера (Ведущий ВК)'

        return new_vc_df[columns]

    def _compare_ap_data(self, xls, is_sharing, ignore_condition=False):
        columns = ['PL', 'SHR_GRAD', 'DLG_GRAD', 'STND', 'AZ1', 'OBL_RUS',
                   'CELL_NAME1', 'CELL_TYPE', 'CELL_STATUS',
                   'UNINSTALL_DATE', 'VVEDENO']

        ap_plan_actual = pd.read_excel(xls)
        ap_plan_actual = ap_plan_actual.rename(columns={
            'Latitude': 'SHR_GRAD',
            'Longitude': 'DLG_GRAD',
            'REG': 'OBL_RUS'
        })

        ap_plan_actual = pd.concat([ap_plan_actual.assign(AZ1=i) for i in (0, 120, 240)])

        if not ignore_condition:
            if is_sharing:
                ap_plan_actual['ADDITIONAL_INVESTMENT_AP_SHAR'] = 'Yes'
                columns.append('ADDITIONAL_INVESTMENT_AP_SHAR')
            else:
                ap_plan_actual['ADDITIONAL_INVESTMENT_AP'] = 'Yes'
                columns.append('ADDITIONAL_INVESTMENT_AP')

        ap_plan_actual['CELL_STATUS'] = 'План'
        ap_plan_actual['CELL_NAME1'] = 'N/A'
        ap_plan_actual['CELL_TYPE'] = 'N/A'
        ap_plan_actual['UNINSTALL_DATE'] = ''
        ap_plan_actual['VVEDENO'] = ''

        return ap_plan_actual[columns]

    def _compare_new_plan_data(self, xls):
        columns = ['PL', 'SHR_GRAD', 'DLG_GRAD', 'STND', 'AZ1', 'REG',
                   'CELL_NAME1', 'CELL_TYPE', 'CELL_STATUS',
                   'UNINSTALL_DATE', 'VVEDENO']

        altai_plan_actual = pd.read_excel(xls)
        altai_plan_actual = altai_plan_actual.rename(columns={
            'Latitude': 'SHR_GRAD',
            'Longitude': 'DLG_GRAD'
        })

        altai_plan_actual = pd.concat([altai_plan_actual.assign(AZ1=i) for i in (0, 120, 240)])
        altai_plan_actual['CELL_STATUS'] = 'План'
        altai_plan_actual['CELL_NAME1'] = 'N/A'
        altai_plan_actual['CELL_TYPE'] = 'N/A'
        altai_plan_actual['UNINSTALL_DATE'] = ''
        altai_plan_actual['VVEDENO'] = ''

        return altai_plan_actual[columns]

    def join_by_slash(self, series):
        unique_values = set(series.to_dict().values())
        not_empty = filter(bool, unique_values)
        return ' / '.join(sorted(map(str, not_empty)))

    def groupe_ranges(self, series):
        unique_values = set(series.to_dict().values())
        not_empty = filter(bool, unique_values)
        return sorted(map(str, not_empty))

    @staticmethod
    def __get_cell_status(row):
        if row['UNINSTALL_DATE']:
            value = 'Демонтаж'
        elif row['VVEDENO']:
            value = 'Эфир'
        else:
            value = 'План'
        return value

    @staticmethod
    def __prepare_cells(row):
        return row['CELL_NAME1'] if row.get('CELL_NAME1') else 'N/A'

    @staticmethod
    def __prepare_controller(row):
        return row['CONTR'] if row.get('CONTR') else 'N/A'

    @staticmethod
    def __prepare_bts(row):
        return row['BTS'] if row.get('BTS') else 'N/A'


start_date = datetime(2020, 4, 8)


dag = DAG(
    dag_id='nioss',
    default_args={
        'owner': 'airflow',
        'start_date': start_date,
        'depends_on_past': False
    },
    schedule_interval='0 23 * * *'
)


PythonOperator(
    task_id='enrich_nioss_topology',
    python_callable=EnrichTopology(),
    dag=dag
)
