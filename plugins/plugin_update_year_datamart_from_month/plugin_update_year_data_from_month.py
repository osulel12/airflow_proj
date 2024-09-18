import clickhouse_connect



class Update_web_app_datamart:
    """
    Класс основной логики обновления витрины годовых данных веб приложения
    """
    def __init__(self, dict_click_cred: dict):
        """
        :param dict_click_cred: словарь с подключением к БД ClickHouse
        """
        # Подключение к базе ClickHouse
        self.click_house_client = clickhouse_connect.get_client(host=dict_click_cred['host'],
                                                                port=dict_click_cred['port'],
                                                                username=dict_click_cred['user'],
                                                                password=dict_click_cred['password'],
                                                                database=dict_click_cred['database'])

    def update_2023_year(self, sql_query: str, year: int, **kwargs):
        """
        :param sql_query: sql скрипт обновления годовых данных из месячных
        :param year: обновляемый год
        :param kwargs: прочие параметры
        """
        sq_month_by_eyar = sql_query.format(year=year)
        self.click_house_client.command(f"""INSERT INTO web_rt.year_app_datamart
                                            {sq_month_by_eyar}""")