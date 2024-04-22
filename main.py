import pandas as pd
import re

def services(df: object) -> object:
    servicedf = pd.DataFrame()
    servicedf.insert(0, 'Услуги', df['Услуги'].str.split(', '))
    servicedf.insert(1, 'Кол-во', df['ФИО']) # Второй столбец чтобы можно было сделать сводную таблицу
    servicedf = servicedf.explode('Услуги').pivot_table(index='Услуги', values='Кол-во', aggfunc='count', fill_value=0)
    return servicedf

def maindf(df: object) -> object:
    df = pd.read_excel('Абоненты.xlsx')
    df.insert(0, 'ФИО', df['Фамилия'] + ' ' + df['Имя'].str[0] + '.' + df['Отчество'].str[0] + '.')
    df = df.drop(['Фамилия', 'Имя', 'Отчество'], axis=1)
    df['Тарифный план'] = df['Тарифный план'].replace('Мбит/с', '', regex=True)
    df['Лицевой счёт'] = df['Лицевой счёт'].replace('-', '', regex=True).replace('=', '', regex=True).astype(int)

    with open('Устройства.txt', "r") as file:
        text = file.read()
    text = text.strip().split('\n\n')
    text = [block.split('\n') for block in text]
    devices = []
    for i in range(0, len(text)-1, 2):
        devices.append(text[i] + text[i+1])
    for device in devices:
        for i in range(1, len(device), 4):
            device.insert(2, *device[i].replace('ю', '.').split(' ')[-1:])
            device[i] = ' '.join(device[i].split(' ')[:-1])
        for i in range(3, len(device), 4):
            device[i] = device[i].strip()
            if device[i].startswith('-'):
                device[i] = device[i].replace(" ", "")
                device[i] = device[i].split("D")[0]
        for i in range(4, len(device), 4):
            device[i] = device[i].split(" ")[0]
    newdf = pd.DataFrame(devices)
    newdf = newdf.rename(columns={0: "Лицевой счёт", 1: 'Модель ONT', 2: "IP", 3: "Уровень сигнала", 4: "Кол-во заявок"})
    newdf['Лицевой счёт'] = newdf['Лицевой счёт'].astype(int)
    df = pd.merge(df, newdf, left_on="Лицевой счёт", right_on="Лицевой счёт")
    df['Кол-во заявок'] = df['Кол-во заявок'].astype(int)
    df['Тарифный план'] = df['Тарифный план'].astype(int)
    return df

def requests(df: object) -> object:
    with open('Заявки.txt', "r") as file:
        text = file.read()
    pattern = r"(.+?)(?=\n\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|\Z)"

    matches = re.findall(pattern, text, re.DOTALL)
    # while "" in matches:
    #     matches.remove("")

    requestsdf = df
    requestsdf = requestsdf.drop("Населённый пункт", axis=1)
    requestsdf = requestsdf.drop("Баланс", axis=1)
    requestsdf = requestsdf.drop("Тарифный план", axis=1)
    requestsdf = requestsdf.drop("Модель ONT", axis=1)
    requestsdf = requestsdf.drop("Уровень сигнала", axis=1)
    newlist = []
    for num in requestsdf['Кол-во заявок']:
        newlist.append(list(range(1, num+1)))
    requestsdf['Номер заявки'] = newlist
    requestsdf = requestsdf.explode('Номер заявки')

    buffdflist = []
    it = 0
    it2 = 0
    nomer = 0
    previp = ''
    for ip in requestsdf['IP']:
        reqlist = []
        reqlist.append(ip)
        if previp != ip:
            it = 0
            it2 = 0
            nomer = 0
        previp = ip
        for match in matches:
            if match.find(ip + ' ') > -1:
                cause = match.replace(ip, '').replace('\n', ' ').replace('- ', '').replace('раз ', '').strip()
                pattern = r"(\d+)\s*([a-zA-Zа-яА-Я\s]+)\s*"
                xd = re.findall(pattern, cause)
                for elem in xd:
                    for str1 in elem:
                        cause = cause.replace(str1, '')
                        # str1 = re.sub(r'(заяв?)', '', str1)
                cause = cause.replace(',', '').strip()
                xd.append(cause)
                if isinstance(xd[it], tuple):
                    reqlist.append(xd[it][1].replace('заявок', '').replace('заявка', '').replace('раза', '').replace('заявки', '').replace('остальные', '').strip())
                else:
                    reqlist.append(xd[it])
                nomer += 1
                reqlist.append(nomer)
                it2 += 1
                if isinstance(xd[it], tuple) and it2 == int(xd[it][0]):
                    it += 1
                    it2 = 0
                buffdflist.append(reqlist)
            elif match.find(ip) > -1:
                cause = match.replace(ip, '').replace('\n', ' ').replace('- ', '').replace('раз ', '').strip()
                pattern = r"(\d+)\s*([a-zA-Zа-яА-Я\s]+)\s*"
                xd = re.findall(pattern, cause)
                for elem in xd:
                    for str1 in elem:
                        cause = cause.replace(str1, '')
                        # str1 = re.sub(r'(заяв?)', '', str1)
                cause = cause.replace(',', '').strip()
                xd.append(cause)
                if isinstance(xd[it], tuple):
                    reqlist.append(xd[it][1].replace('заявок', '').replace('заявка', '').replace('раза', '').replace('заявки', '').strip())
                else:
                    reqlist.append(xd[it].replace('остальные ', ''))
                nomer += 1
                reqlist.append(nomer)
                it2 += 1
                if isinstance(xd[it], tuple) and it2 == int(xd[it][0]):
                    it += 1
                    it2 = 0
                buffdflist.append(reqlist)
    buffdf = pd.DataFrame(buffdflist)
    buffdf = buffdf.rename(columns={0: "IP", 1: 'Причина возникновения заявки', 2: "Номер заявки"})
    requestsdf = pd.merge(requestsdf, buffdf, left_on=["IP", "Номер заявки"], right_on=["IP", "Номер заявки"])

    requestsdf = requestsdf.drop("Кол-во заявок", axis=1)
    requestsdf = requestsdf.drop("IP", axis=1)
    requestsdf = requestsdf.dropna()
    requestsdf['Причина возникновения заявки'] = requestsdf['Причина возникновения заявки'].replace('все заявки ', '', regex=True)
    requestsdf['Причина возникновения заявки'] = requestsdf['Причина возникновения заявки'].replace('первое и второе ', '', regex=True)

    return requestsdf

def main():
    df = maindf(pd.read_excel('Абоненты.xlsx'))
    servicedf = services(df)
    requestsdf = requests(df)
    print(df)
    print(requestsdf)
    print(servicedf)
    
    with pd.ExcelWriter('Абоненты_Результат.xlsx') as writer:
        df.to_excel(writer, sheet_name='Абоненты', index=False)
        worksheet = writer.sheets['Абоненты']
        worksheet.set_column('A:J', 20)
        servicedf.to_excel(writer, sheet_name='Услуги', index=True)
        requestsdf.to_excel(writer, sheet_name='Заявки', index=False)
        worksheet = writer.sheets['Заявки']
        worksheet.set_column('A:J', 30)

if __name__ == "__main__":
    main()