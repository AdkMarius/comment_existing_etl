from collections import Counter
import logging
from logging_utils import setup_logging
from collections import defaultdict
from datetime import datetime, timedelta

setup_logging()


def rows_tolist(datos):
    """
    Convert each row of a DataFrame to a list and append to a list of rows.

    Args:
        datos: A pandas DataFrame.

    Returns:
        A list of lists, where each inner list represents a row from the DataFrame.
    """
    lista_filas = []
    logging.info(f"passing each row to the list")
    for indice, fila in datos.iterrows():
        lista_filas.append(fila.tolist())
    return lista_filas


def info_death(records):
    """
    Extract death-related data from records.

    Args:
        records: A list of records from the death model.

    Returns:
        Three lists: death dates, necropsy dates, and tuples of death dates with cause of death names.
    """
    logging.info("Obtaining data from the death model")
    deathillnes = [(rec.dod, rec.cod.name) for rec in records]
    death = [rec.dod for rec in records]
    necropsy = [rec.dod for rec in records if rec.autopsy]
    return death, necropsy, deathillnes


def info_disease(record):
    """
    Extract disease-related data from records.

    Args:
        record: A list of records from the disease model.

    Returns:
        Three lists: confirmed cases, suspected cases, and recovered cases with diagnosis dates and pathology names.
    """
    logging.info("Obtaining data from the disease model")
    confir = [(rec.diagnosed_date, rec.pathology.name) for rec in record if
              rec.lab_confirmed and rec.healed_date is None]
    suspe = [(rec.diagnosed_date, rec.pathology.name) for rec in record if
             not rec.lab_confirmed and rec.healed_date is None]
    reco = [(rec.healed_date, rec.pathology.name) for rec in record if
            rec.lab_confirmed and rec.healed_date is not None]
    return confir, suspe, reco


def info_registration(recor):
    """
    Extract registration-related data from records.

    Args:
        recor: A list of records from the registration model.

    Returns:
        Several lists containing dates and reasons related to hospitalizations, ICU admissions, discharges, etc.
    """
    logging.info("Obtaining data from the registration model")
    bedh = [(rec.hospitalization_date, rec.admission_reason.name) for rec in recor]
    bedicu = [(rec.hospitalization_date, rec.admission_reason.name) for rec in recor if rec.icu]
    dish = [(rec.discharge_date, rec.admission_reason.name) for rec in recor if rec.state != 'hospitalized']
    dispc = [(rec.discharge_date, rec.admission_reason.name) for rec in recor if rec.discharge_reason == 'Home']
    ocu = [(rec.discharge_date, rec.admission_reason.name) for rec in recor if rec.bed.state == 'occupied']
    tuover = [(rec.hospitalization_date) for rec in recor]
    esta = [(rec.discharge_date, rec.hospitalization_date, (rec.discharge_date - rec.hospitalization_date).days) for rec
            in recor]
    return bedh, bedicu, dish, dispc, ocu, tuover, esta


def info_newborn(recor):
    """
    Extract newborn-related data from records.

    Args:
        recor: A list of records from the pregnancy model.

    Returns:
        Several lists containing dates of pregnancies ending in live birth, not live birth, etc.
    """
    logging.info("Obtaining data from the pregnancy model")
    newb = [rec.pregnancy_end_date for rec in recor if rec.pregnancy_end_result == 'live_birth']
    newbdied = [rec.pregnancy_end_date for rec in recor if rec.pregnancy_end_result != 'live_birth']
    born = [rec.pregnancy_end_date for rec in recor]
    cae = [rec.pregnancy_end_date for rec in recor if rec.perinatal[0].start_labor_mode == 'c']
    vag = [rec.pregnancy_end_date for rec in recor if rec.perinatal[0].start_labor_mode != 'c']
    return newb, newbdied, born, cae, vag


def info_surgery(recor):
    """
    Extract surgery-related data from records.

    Args:
        recor: A list of records from the surgery model.

    Returns:
        Several lists containing dates of surgeries, surgeries resulting in death, surgeries with complications, etc.
    """
    logging.info("Obtaining data from the surgery model")
    surg = [rec.surgery_end_date for rec in recor]
    surgdied = [rec.surgery_end_date for rec in recor if rec.clavien_dindo != 'grade1']
    surgalive = [rec.surgery_end_date for rec in recor if rec.clavien_dindo == 'grade1']
    weekend_surg = [date for date in surg if date.weekday() in [5, 6]]
    weekend_surgalive = [date for date in surgalive if date.weekday() in [5, 6]]
    return surg, surgdied, surgalive, weekend_surg, weekend_surgalive


def transf_dict(surg):
    """
    Convert a list of dates into a dictionary with the count of each date.

    Args:
        surg: A list of datetime objects.

    Returns:
        A dictionary with dates (as strings) as keys and their counts as values.
    """

    # Truncate dates to only the date component (ignoring the time)
    fechas_sin_hora = [fecha.date() for fecha in surg]

    # Count the frequency of each truncated date.
    frecuencia_fechas = Counter(fechas_sin_hora)

    # Convertir el resultado a un diccionario con fechas formateadas como cadenas
    diccionario_fechas = {fecha.strftime('%Y-%m-%d'): count for fecha, count in frecuencia_fechas.items()}
    return diccionario_fechas


def dict_payload(morta, result):
    """
    Create a dictionary payload from mortality data.

    Args:
        morta: A dictionary with date keys and counts as values.
        result: The data element to be included in the payload.

    Returns:
        A dictionary with date keys and a nested dictionary containing the data element and value.
    """
    nuevo_diccionario = {}

    # Browse the original dictionary
    for clave, valor in morta.items():
        nuevo_diccionario[clave] = {'dataElement': result, 'value': valor}
    return nuevo_diccionario


def trans_dict(muertes, lista_valores_unicos):
    """
    Transform death data into a dictionary with counts per disease.

    Args:
        muertes: A list of tuples with death dates and disease names.
        lista_valores_unicos: A list of unique disease names.

    Returns:
        A dictionary with disease names as keys and dictionaries of dates with counts as values.
    """

    # Create a dictionary to store dates by disease
    fechas_por_enfermedad = {enf.lower(): [] for enf in lista_valores_unicos}

    # Iterate on death data and add dates to the corresponding disease
    for fecha, enfermedad in muertes:
        for enf in lista_valores_unicos:
            if enf.lower() in enfermedad.lower():
                # Check if the date is already on the disease list
                if fecha not in fechas_por_enfermedad[enf.lower()]:
                    # If it is not in the list, add it
                    fechas_por_enfermedad[enf.lower()].append(fecha)

    # Create a dictionary to store the final count
    diccionario_final = {}

    # Iterate on each disease and its corresponding dates.
    for enfermedad, fechas in fechas_por_enfermedad.items():
        if not fechas:
            # If empty, assign an empty dictionary to the count.
            conteo_fechas = {}
        else:
            # If not empty, count dates using Counter
            conteo_fechas = dict(Counter(map(lambda x: x.strftime('%Y-%m-%d'), fechas)))

        # Storing the count in the new dictionary
        diccionario_final[enfermedad] = conteo_fechas
    return diccionario_final


def convertir_a_primer_dia_semana(fecha):
    """
    Convert a date to the first day of its week.

    Args:
        fecha: A string representing a date in the format "YYYY-MM-DD".

    Returns:
        A string representing the first day of the week in the format "YYYYWww".
    """
    dt = datetime.strptime(fecha, "%Y-%m-%d")
    inicio_semana = dt - timedelta(days=dt.weekday())
    return inicio_semana.strftime("%YW%U")


def weekly(ok):
    """
    Convert daily data to weekly data by aggregating counts per week.

    Args:
        ok: A dictionary with disease names as keys and dictionaries of dates with counts as values.

    Returns:
        A dictionary with disease names as keys and dictionaries of weeks with aggregated counts as values.
    """

    # Convert dates to “YYYYY-Www” and group by week
    for enfermedad, fechas in ok.items():
        fechas_convertidas = {}
        for fecha, valor in fechas.items():
            semana = convertir_a_primer_dia_semana(fecha)
            if semana in fechas_convertidas:
                fechas_convertidas[semana] += valor
            else:
                fechas_convertidas[semana] = valor
        ok[enfermedad] = fechas_convertidas
    return ok


def to_week(a):
    """
    Convert daily data to weekly data by aggregating counts per week.

    Args:
        a: A dictionary with dates as keys and counts as values.

    Returns:
        A dictionary with weeks as keys and aggregated counts as values.
    """
    nuevo_diccionario = {}
    for fecha, valor in a.items():
        # Convert the date to a datetime object
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d')

        # Calcular la semana del año (ejemplo: '2014W27')
        semana_del_ano = f'{fecha_obj.year}W{fecha_obj.strftime("%U")}'
        nuevo_diccionario[semana_del_ano] = nuevo_diccionario.get(semana_del_ano, 0) + valor
    return nuevo_diccionario


def sorteddict(a):
    """
    Sort a dictionary by its keys.

    Args:
        a: A dictionary.

    Returns:
        A sorted dictionary.
    """
    return dict(sorted(a.items()))


def sumar(ejemplo, ejemplo2):
    """
    Sum the values of two dictionaries.

    Args:
        ejemplo: The first dictionary.
        ejemplo2: The second dictionary.

    Returns:
        A dictionary with summed values for each key.
    """
    resultado = {clave: dic.copy() for clave, dic in ejemplo.items()}

    # Iterate over the second dictionary and add the amounts
    for clave, dic in ejemplo2.items():
        if clave in resultado:
            for subclave, cantidad in dic.items():
                # Use the get method to get the current value or 0 if the subclave does not exist
                resultado[clave][subclave] = resultado[clave].get(subclave, 0) + cantidad
        else:
            # If the key is not in the first dictionary, add it with its subkeys and amounts
            resultado[clave] = dict(dic)

    return resultado

def dividir(ejemplo1, ejemplo2):
    """
    Divide the values of one dictionary by another.

    Args:
        ejemplo1: The first dictionary.
        ejemplo2: The second dictionary.

    Returns:
        A dictionary with divided values for each key.
    """
    # Create a copy of the second dictionary to avoid modifying the original
    resultado = {clave: dic.copy() for clave, dic in ejemplo2.items()}

    # Iterate over the first dictionary and divide the amounts
    for clave, dic in ejemplo1.items():
        if clave in resultado:
            for subclave, cantidad in dic.items():
                # Use the get method to get the current value or 1 if the subclave does not exist
                divisor = resultado[clave].get(subclave, 1)
                resultado[clave][subclave] = cantidad / divisor
        else:
            # If the key is not in the second dictionary, add it with its subkeys and amounts
            resultado[clave] = {subclave: cantidad for subclave, cantidad in dic.items()}

    return resultado

def porcentaje(ejemplo1, ejemplo2):
    """
    Calculate the percentage of one dictionary's values over another's.

    Args:
        ejemplo1: The first dictionary.
        ejemplo2: The second dictionary.

    Returns:
        A dictionary with percentage values for each key.
    """
    # Create a copy of the second dictionary to avoid modifying the original
    resultado = {clave: dic.copy() for clave, dic in ejemplo2.items()}

    # Iterate over the first dictionary and divide the amounts
    for clave, dic in ejemplo1.items():
        if clave in resultado:
            for subclave, cantidad in dic.items():
                # Use the get method to get the current value or 1 if the subclave does not exist
                divisor = resultado[clave].get(subclave, 1)
                resultado[clave][subclave] = (cantidad / divisor) * 100
        else:
            # If the key is not in the second dictionary, add it with its subkeys and amounts
            resultado[clave] = {subclave: cantidad for subclave, cantidad in dic.items()}

    return resultado

def sumar_tres_diccionarios(ejemplo1, ejemplo2, ejemplo3):
    """
    Sum the values of three dictionaries.

    Args:
        ejemplo1: The first dictionary.
        ejemplo2: The second dictionary.
        ejemplo3: The third dictionary.

    Returns:
        A dictionary with summed values for each key.
    """
    # Create a result dictionary as a shallow copy of the first dictionary
    resultado = {clave: dic.copy() for clave, dic in ejemplo1.items()}

    # Iterate over the second dictionary and add the amounts
    for clave, dic in ejemplo2.items():
        if clave in resultado:
            for subclave, cantidad in dic.items():
                resultado[clave][subclave] = resultado[clave].get(subclave, 0) - cantidad
        else:
            resultado[clave] = dict(dic)

    # Iterate over the third dictionary and add the amounts
    for clave, dic in ejemplo3.items():
        if clave in resultado:
            for subclave, cantidad in dic.items():
                resultado[clave][subclave] = resultado[clave].get(subclave, 0) - cantidad
        else:
            resultado[clave] = dict(dic)

    return resultado

def transformar_lista(fechas, elemen):
    """
    Transform a list of dates into a specific format.

    Args:
        fechas: A list of date objects.
        elemen: The data element to be included in the result.

    Returns:
        A list of dictionaries with the transformed dates and their counts.
    """
    resultado = []
    contador_fechas = {}

    for fecha in fechas:
        year, week, _ = fecha.isocalendar()
        clave = f'{year}W{week:02}'

        if clave in contador_fechas:
            contador_fechas[clave] += 1
        else:
            contador_fechas[clave] = 1

        elemento_existente = next((elem for elem in resultado if clave in elem), None)

        if elemento_existente:
            elemento_existente[clave]['value'] = contador_fechas[clave]
        else:
            elemento = {clave: {'dataElement': elemen, 'value': contador_fechas[clave]}}
            resultado.append(elemento)

    return resultado

def transfor(esta, elemen):
    """
    Transform a list of tuples into a specific format.

    Args:
        esta: A list of tuples with dates and differences.
        elemen: The data element to be included in the result.

    Returns:
        A list of dictionaries with the highest value per week.
    """
    sorted_date_differences = sorted(esta, key=lambda x: x[0])
    accumulated_list = [(tupla[0], tupla[1], sum(item[2] for item in sorted_date_differences[:i + 1])) for i, tupla in
                        enumerate(sorted_date_differences)]
    result_list = [(tupla[0], tupla[2]) for tupla in accumulated_list]
    formatted_result_list = [(date.strftime('%YW%W'), value) for date, value in result_list]
    max_value_dict = {}

    # Calculate the highest value per week
    for week, value in formatted_result_list:
        if week in max_value_dict:
            max_value_dict[week] = max(max_value_dict[week], value)
        else:
            max_value_dict[week] = value

    # Create a new list with the highest value per week
    result_list = [{week: {'dataElement': elemen, 'value': float(max_value)}} for week, max_value in
                   max_value_dict.items()]

    return result_list

def stay(result_list, abc, elemen):
    """
    Calculate the ratio of values from two lists of dictionaries.

    Args:
        result_list: The first list of dictionaries.
        abc: The second list of dictionaries.
        elemen: The data element to be included in the result.

    Returns:
        A list of dictionaries with the calculated ratios.
    """
    resultados = []

    # Create dictionaries to facilitate date lookup in both lists
    diccionario_lista1 = {list(d.keys())[0]: d for d in result_list}
    diccionario_lista2 = {list(d.keys())[0]: d for d in abc}

    # Get all unique dates
    fechas_unicas = set(diccionario_lista1.keys()).union(diccionario_lista2.keys())

    # Iterate over all dates and calculate the results
    for fecha in fechas_unicas:
        valor1 = diccionario_lista1.get(fecha, {}).get(fecha, {}).get('value', 1)
        valor2 = diccionario_lista2.get(fecha, {}).get(fecha, {}).get('value', 1)

        resultado = {fecha: {'dataElement': elemen, 'value': valor1 / valor2}}
        resultados.append(resultado)
    lista_original = sorted(resultados, key=lambda x: list(x.keys())[0])
    diccionario_grande = {}

    for diccionario_pequeno in lista_original:
        diccionario_grande.update(diccionario_pequeno)

    lista_resultante = [diccionario_grande]

    return lista_resultante

def res_ind(dic1, dic2):
    """
    Subtract the values of two dictionaries.

    Args:
        dic1: The first dictionary.
        dic2: The second dictionary.

    Returns:
        A dictionary with subtracted values for each key.
    """
    # Create a new dictionary to store the subtracted values by date
    result_dict_solv = {}

    # Iterate over common dates in both dictionaries
    common_dates = set(dic1.keys()) & set(dic2.keys())
    for date in common_dates:
        result_dict_solv[date] = dic1[date] - dic2[date]

    # Add dates that are only in dic1
    for date in set(dic1.keys()) - common_dates:
        result_dict_solv[date] = dic1[date]

    # Add dates that are only in dic2
    for date in set(dic2.keys()) - common_dates:
        result_dict_solv[date] = dic2[date]
    return result_dict_solv

def percent_ind(dic1, dic2):
    """
    Calculate the percentage of values from two dictionaries.

    Args:
        dic1: The first dictionary.
        dic2: The second dictionary.

    Returns:
        A dictionary with percentage values for each key.
    """
    # Create a new dictionary to store the percentage values by date
    result_dict_solv = {}

    # Iterate over common dates in both dictionaries
    common_dates = set(dic1.keys()) & set(dic2.keys())
    for date in common_dates:
        result_dict_solv[date] = (dic1[date] / dic2[date]) * 100

    # Add dates that are only in dic1
    for date in set(dic1.keys()) - common_dates:
        result_dict_solv[date] = dic1[date]

    # Add dates that are only in dic2
    for date in set(dic2.keys()) - common_dates:
        result_dict_solv[date] = dic2[date]
    return result_dict_solv

def percentmil_ind(dic1, dic2):
    """
    Calculate the percentage per thousand of values from two dictionaries.

    Args:
        dic1: The first dictionary.
        dic2: The second dictionary.

    Returns:
        A dictionary with percentage per thousand values for each key.
    """
    # Create a new dictionary to store the percentage per thousand values by date
    result_dict_solv = {}

    # Iterate over common dates in both dictionaries
    common_dates = set(dic1.keys()) & set(dic2.keys())
    for date in common_dates:
        result_dict_solv[date] = (dic1[date] / dic2[date]) * 1000

    # Add dates that are only in dic1
    for date in set(dic1.keys()) - common_dates:
        result_dict_solv[date] = dic1[date]

    # Add dates that are only in dic2
    for date in set(dic2.keys()) - common_dates:
        result_dict_solv[date] = dic2[date]
    return result_dict_solv

def div_ind(dic1, dic2):
    """
    Divide the values of two dictionaries.

    Args:
        dic1: The first dictionary.
        dic2: The second dictionary.

    Returns:
        A dictionary with divided values for each key.
    """
    # Create a new dictionary to store the divided values by date
    result_dict_solv = {}

    # Iterate over common dates in both dictionaries
    common_dates = set(dic1.keys()) & set(dic2.keys())
    for date in common_dates:
        result_dict_solv[date] = (dic1[date] / dic2[date])

    # Add dates that are only in dic1
    for date in set(dic1.keys()) - common_dates:
        result_dict_solv[date] = dic1[date]

    # Add dates that are only in dic2
    for date in set(dic2.keys()) - common_dates:
        result_dict_solv[date] = dic2[date]
    return result_dict_solv

def sortedict(data):
    """
    Sort the values of a dictionary by its keys.

    Args:
        data: The dictionary to be sorted.

    Returns:
        A dictionary with sorted values.
    """
    for disease, dates in data.items():
        data[disease] = dict(sorted(dates.items()))
    return data

def accumulated(data):
    """
    Accumulate the values of a dictionary.

    Args:
        data: The dictionary to be accumulated.

    Returns:
        A dictionary with accumulated values.
    """
    for disease, dates in data.items():
        cumulative_count = 0
        for date, count in dates.items():
            cumulative_count += count
            data[disease][date] = cumulative_count
    return data

def new_dict(dicc_act):
    """
    Transform a dictionary into a new format.

    Args:
        dicc_act: The original dictionary.

    Returns:
        A new dictionary with the desired structure.
    """
    nuevo_diccionario = {}
    # Iterate over the original dictionary
    for clave, valor in dicc_act.items():
        for fecha, cantidad in valor.items():
            nuevo_diccionario[fecha] = {'dataElement': clave, 'value': cantidad}
    return nuevo_diccionario

def new_dict_dupli(dicc_act):
    """
    Transform a dictionary into a new format with duplicates.

    Args:
        dicc_act: The original dictionary.

    Returns:
        A list of dictionaries with the desired structure.
    """
    nuevo_diccionario = {}

    # Iterate over the original dictionary
    for enfermedad, fechas in dicc_act.items():
        for fecha, cantidad in fechas.items():
            # Create a new dictionary with the desired structure
            if cantidad > 0:  # Ensure to add only dates with quantity greater than 0
                clave = f"{fecha}-{enfermedad}"
                if fecha not in nuevo_diccionario:
                    nuevo_diccionario[fecha] = [{'dataElement': enfermedad, 'value': cantidad}]
                else:
                    nuevo_diccionario[fecha].append({'dataElement': enfermedad, 'value': cantidad})

    # Convert the final dictionary to a list of dictionaries
    lista_final = []
    for fecha, registros in nuevo_diccionario.items():
        for registro in registros:
            lista_final.append({fecha: [registro]})

    return lista_final

def dict_in_list(nuevo_diccionario):
    """
    Convert a dictionary into a list.

    Args:
        nuevo_diccionario: The original dictionary.

    Returns:
        A list containing the dictionary.
    """
    lista = []

    lista.append(nuevo_diccionario)
    return lista

def dict_to_list(list_illnes, vector):
    """
    Convert a dictionary into a list format.

    Args:
        list_illnes: A list of illnesses.
        vector: A vector of values.

    Returns:
        A new dictionary in the desired list format.
    """
    sector = dict(zip(list_illnes, vector.values()))
    new_dict1 = new_dict_dupli(sector)
    return new_dict1

def new_dict_f(dicc_act):
    """
    Transform a dictionary into a new format with duplicates (alternative function).

    Args:
        dicc_act: The original dictionary.

    Returns:
        A list of dictionaries with the desired structure.
    """
    nuevo_diccionario = {}

    # Iterate over the original dictionary
    for enfermedad, fechas in dicc_act.items():
        for fecha, cantidad in fechas.items():
            # Create a new dictionary with the desired structure
            if cantidad > 0:  # Ensure to add only dates with quantity greater than 0
                clave = f"{fecha}-{enfermedad}"
                if fecha not in nuevo_diccionario:
                    nuevo_diccionario[fecha] = [{'dataElement': enfermedad, 'value': cantidad}]
                else:
                    nuevo_diccionario[fecha].append({'dataElement': enfermedad, 'value': cantidad})

    # Convert the final dictionary to a list of dictionaries
    lista_final = []
    for fecha, registros in nuevo_diccionario.items():
        for registro in registros:
            lista_final.append({fecha: [registro]})

    return lista_final

def dictio_to_list(list_illnes, vector):
    """
    Convert a dictionary into a specific list format.

    Args:
        list_illnes: A list of illnesses.
        vector: A vector of values.

    Returns:
        A transformed list of dictionaries.
    """
    sector = dict(zip(list_illnes, vector.values()))
    new_dict1 = new_dict_f(sector)
    lista_transformada = [{clave: diccionario[0]} for diccionario_con_lista in new_dict1
                          for clave, diccionario in diccionario_con_lista.items()]
    listas_listas = [[elementos] for elementos in lista_transformada]
    return listas_listas
