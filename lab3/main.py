import pickle, csv, os
from itertools import islice

with open('file.pickle', 'wb') as f:
    
    pickle.dump([
    ['Название игры', 'Жанр', 'Год выпуска', 'Оценка'],
    ['Cyberpunk 2077', 'RPG', 2020, 7.5],
    ['The Witcher 3', 'RPG', 2015, 9.5],
    ['Minecraft', 'Песочница', 2011, 9.0]
    ], f)

with open('file.pickle', 'rb') as f:
    data = pickle.load(f)
print(*data, sep='\n')


table = [
    ['Название сериала', 'Жанр', 'Год выхода', 'Рейтинг'],
    ['Игра престолов', 'Фэнтези', 2011, 9.3],
    ['Во все тяжкие', 'Криминал', 2008, 9.5],
    ['Чернобыль', 'Драма', 2019, 9.4],
]

with open('file.csv', mode='w', encoding='utf-8') as f:
    file_writer = csv.writer(f, delimiter = ',', lineterminator='\r')
    for r in table:
        file_writer.writerow(r)


with open('file.csv', 'r', encoding = 'utf-8') as f:
    reader = csv.reader(f)
    table = [r for r in reader]

print(*table, sep = "\n")

class MFT:
    def __init__(self, tof):
        self.tof = tof
        self.headers = []
        self.rows = []

    def chunked_rows(self, rows, chunk_size):
        it = iter(rows)
        while chunk := list(islice(it, chunk_size)):
            yield chunk

    def load_csv_file(self, *fn):
        list_headers = []  
        all_rows = []      
    
        for file_name in fn:
            with open(f'{file_name}.csv', encoding='utf-8') as f:
                reader = csv.reader(f)
                
                headers = tuple(next(reader))
                list_headers.append(headers)
    
                for row in reader:
                    all_rows.append(row)
    
        if len(set(list_headers)) != 1:
            raise Exception("Заголовки не совпадают")
    
        self.headers = list(list_headers[0])
        self.rows = all_rows  

    def save_csv_file(self, file_name, max_rows_per_file):
        file_number = 1  
        for chunk in self.chunked_rows(self.rows, max_rows_per_file):
            current_file = f"{file_name}_{file_number}.csv"
            with open(current_file, mode='w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(self.headers)  
                writer.writerows(chunk)  
            file_number += 1

    def load_pickle_file(self, *fn):
        list_headers = []  
        all_rows = []      
    
        for file_name in fn:
            with open(f'{file_name}.pickle', 'rb') as f:
    
                data = pickle.load(f)
                if len(data) != 2 or not isinstance(data[0], list) or not isinstance(data[1], list):
                    raise Exception("Некорректный формат данных в pickle файле")

                headers = tuple(data[0])  
                list_headers.append(headers)
                
                all_rows.extend(data[1])  
    
        if len(set(list_headers)) != 1:
            raise Exception("Заголовки не совпадают")
    
        self.headers = list_headers[0]  
        self.rows = all_rows      

    def save_pickle_file(self, file_name, max_rows_per_file):
        file_number = 1
        for chunk in self.chunked_rows(self.rows, max_rows_per_file):
            current_file = f"{file_name}_{file_number}.pickle"
            with open(current_file, mode='wb') as file:
                pickle.dump([self.headers, chunk], file)
            file_number += 1

    def save_txt_file(self, fn, max_rows_per_file):
        for file_number, start in enumerate(range(0, len(self.rows), max_rows_per_file), start=1):
            with open(f"{fn}_{file_number}.txt", mode='w', encoding='utf-8') as file:
                file.write("\t".join(self.headers) + "\n")
                file.writelines("\t".join(map(str, row)) + "\n" for row in self.rows[start:start + max_rows_per_file])

    def load_file(self, *fn):
        if self.tof == 'csv':
            self.load_csv_file(*fn)
        elif self.tof == 'pickle':
            self.load_pickle_file(*fn)
        else:
            raise Exception('Такого типа файла нет')
        
        max_file_size = 10 * 1024 * 1024  
        max_rows = 100000  
        for file_name in fn:
            file_path = f"{file_name}.{self.tof}"
            if os.path.getsize(file_path) > max_file_size:
                raise Exception(f"Файл {file_path} слишком большой")

            with open(file_path, mode='r', encoding='utf-8') as file:
                line_count = sum(1 for _ in file)
            if line_count > max_rows + 1:  
                raise Exception(f"В файле {file_path} слишком много строк")

    def save_file(self, fn, max_rows_per_file):
        if self.tof == 'csv':
            self.save_csv_file(fn, max_rows_per_file)
        elif self.tof == 'pickle':
            self.save_pickle_file(fn, max_rows_per_file)
        elif self.tof == 'txt':
            self.save_txt_file(fn, max_rows_per_file)
        else:
            raise Exception('Такого типа файла нет')
            
        file_number = 1
        while True:
            file_path = f"{fn}_{file_number}.{self.tof}"
            if not os.path.exists(file_path):
                break  

    def print_table(self):
        print(*self.headers)
        for row in self.rows:
            print(*row)

    def get_value(self, column=0):
        values = self.get_values(column)
        if not values:
            return None
        return values[0]

    def get_rows_by_number(self, start, stop=None, copy_table=False):
        try:
            if not isinstance(start, int) or (stop is not None and not isinstance(stop, int)):
                raise ValueError("start и stop должны быть целыми числами")
            if start < 0 or (stop is not None and stop < 0):
                raise IndexError("start и stop не могут быть отрицательными числами")
            if start >= len(self.rows) or (stop is not None and stop >= len(self.rows)):
                raise IndexError("start или stop выходит за пределы доступных строк")
            if stop is None:
                rows = [self.rows[start]]
            else:
                if start > stop:
                    raise ValueError("start не может быть больше stop")
                rows = self.rows[start:stop+1]
            if copy_table:
                rows = [row.copy() for row in rows]
            return rows
        except (IndexError, ValueError) as e:
            print(e)
            return None

    def get_rows_by_index(self, *values, copy_table=False):
        try:
            if len(values) == 0:
                raise ValueError("Не указаны значения для поиска в первой колонке")
            result_rows = []
            for row in self.rows:
                if row[0] in values:
                    result_rows.append(row.copy() if copy_table else row)
            return result_rows
        except ValueError as e:
            print(e)
            return None

    def get_column_types(self, by_number=True):
        column_types = {}
        for col_index in range(len(self.headers)):
            values = []
            for row in self.rows:
                if len(row) > col_index:
                    values.append(row[col_index])
            col_type = str  # Default type
            if all(isinstance(val, bool) or str(val).lower() in ["true", "false"] for val in values if val != ""):
                col_type = bool
            elif all(isinstance(val, int) or (str(val).isdigit() and val != "") for val in values):
                col_type = int
            elif all(isinstance(val, float) or self.isfloat(val) for val in values if val != ""):
                col_type = float
            elif all(self.is_datetime(val) for val in values if val != ""):
                col_type = datetime
            if by_number:
                column_types[col_index] = col_type
            else:
                column_types[self.headers[col_index]] = col_type
        return column_types

    def isfloat(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def is_datetime(self, value):
        try:
            if isinstance(value, datetime):
                return True
            datetime.fromisoformat(value)  # Assuming ISO 8601 format
            return True
        except (ValueError, TypeError):
            return False

    def set_column_types(self, types_dict, by_number=True):
        for key, value_type in types_dict.items():
            column_index = key if by_number else self.headers.index(key)
            for row_index, row in enumerate(self.rows):
                try:
                    cell_value = row[column_index]
                    if value_type is int:
                        row[column_index] = int(cell_value)
                    elif value_type is float:
                        row[column_index] = float(cell_value)
                    elif value_type is bool:
                        row[column_index] = str(cell_value).lower() in ["true", "1"]
                    elif value_type is str:
                        row[column_index] = str(cell_value)
                    elif value_type is datetime:
                        if isinstance(cell_value, datetime):
                            row[column_index] = cell_value
                        else:
                            row[column_index] = datetime.fromisoformat(cell_value)
                    else:
                        raise ValueError("Такого типа данных нет")
                except (ValueError, IndexError) as e:
                    print(f"Ошибка в строке {row_index + 1}, столбце {column_index + 1}: {e}")

    def get_values(self, column=0):
        if isinstance(column, int):
            column_index = column
        elif isinstance(column, str):
            if column in self.headers:
                column_index = self.headers.index(column)
            else:
                print("Столбец не найден")
                return []
        else:
            print("Передан неверный тип столбца")
            return []
        if column_index < 0 or column_index >= len(self.headers):
            print("Индекс столбца выходит за границы")
            return []
        result = []
        for row in self.rows:
            if len(row) > column_index:  
                result.append(row[column_index])
            else:
                result.append(None)  
        return result

    def set_values(self, values, column=0):
        if not isinstance(values, list) or not values:
            raise ValueError("values должен быть непустым списком")
        column_index = column if isinstance(column, int) else self.headers.index(column) if isinstance(column, str) and column in self.headers else None
        if column_index is None:
            raise ValueError(f"Неверный столбец: {column}. Столбец с таким именем не найден.")
        if len(values) != len(self.rows):
            raise ValueError(f"Количество значений в values ({len(values)}) не совпадает с количеством строк в таблице ({len(self.rows)}).")
        column_type = type(self.rows[0][column_index])
        for i, value in enumerate(values):
            try:
                if column_type is int:
                    self.rows[i][column_index] = int(value)
                elif column_type is float:
                    self.rows[i][column_index] = float(value)
                elif column_type is bool:
                    self.rows[i][column_index] = str(value).lower() in {"true", "1"}
                elif column_type is str:
                    self.rows[i][column_index] = str(value)
            except ValueError as e:
                raise ValueError(f"Не удалось преобразовать значение '{value}' в тип {column_type}: {e}")

    def set_value(self, value, column=0):
        if isinstance(column, int):
            if column < len(self.headers):
                column_type = type(self.rows[0][column])
                try:
                    if column_type is int:
                        self.rows[0][column] = int(value)
                    elif column_type is float:
                        self.rows[0][column] = float(value)
                    elif column_type is bool:
                        self.rows[0][column] = str(value).lower() in {"true", "1"}
                    elif column_type is str:
                        self.rows[0][column] = str(value)
                except ValueError as e:
                    raise ValueError(f"Не удалось преобразовать значение '{value}' в тип {column_type}: {e}")
            else:
                raise IndexError('Индекс столбца выходит за границы.')
        elif isinstance(column, str):
            if column in self.headers:
                col_index = self.headers.index(column)
                column_type = type(self.rows[0][col_index])
                try:
                    if column_type is int:
                        self.rows[0][col_index] = int(value)
                    elif column_type is float:
                        self.rows[0][col_index] = float(value)
                    elif column_type is bool:
                        self.rows[0][col_index] = str(value).lower() in {"true", "1"}
                    elif column_type is str:
                        self.rows[0][col_index] = str(value)
                except ValueError as e:
                    raise ValueError(f"Не удалось преобразовать значение '{value}' в тип {column_type}: {e}")
            else:
                raise ValueError(f"Столбец '{column}' не найден.")
        else:
            raise TypeError("Колонка должна быть числом или строкой")

    def concat(self, table1, table2):
        if table1.headers != table2.headers:
            raise ValueError("Заголовки таблиц не совпадают")
        new_table = MFT(table1.tof)
        new_table.headers = table1.headers
        new_table.rows = table1.rows + table2.rows
        return new_table

    def split(self, row_number):
        if not (0 <= row_number < len(self.rows)):
            raise IndexError("Номер строки выходит за пределы таблицы")
        table1 = MFT(self.tof)
        table2 = MFT(self.tof)
        table1.headers = self.headers
        table2.headers = self.headers
        table1.rows = self.rows[:row_number]
        table2.rows = self.rows[row_number:]
        return table1, table2
    def merge_tables(table1, table2, by_number=True, conflict_resolution='raise'):
        headers1, *rows1 = table1
        headers2, *rows2 = table2
        
        all_headers = list(dict.fromkeys(headers1 + headers2))  
        header_index_map = {header: i for i, header in enumerate(all_headers)}
        

        merged_table = [all_headers]
        
        if by_number:

            max_rows = max(len(rows1), len(rows2))
            for i in range(max_rows):
                row1 = rows1[i] if i < len(rows1) else [None] * len(headers1)
                row2 = rows2[i] if i < len(rows2) else [None] * len(headers2)
                merged_row = [None] * len(all_headers)
                
                for header in headers1:
                    merged_row[header_index_map[header]] = row1[headers1.index(header)]
                for header in headers2:
                    idx = header_index_map[header]
                    if merged_row[idx] is not None and row2[headers2.index(header)] is not None:
                        if merged_row[idx] != row2[headers2.index(header)]:
                            if conflict_resolution == 'table1':
                                pass  
                            elif conflict_resolution == 'table2':
                                merged_row[idx] = row2[headers2.index(header)]
                            elif conflict_resolution == 'raise':
                                raise ValueError(f"Conflict at row {i}, column '{header}'")
                    else:
                        merged_row[idx] = row2[headers2.index(header)]
                
                merged_table.append(merged_row)
        else:
            index_map = {}
            for i, row in enumerate(rows1):
                if len(row) > 0 and row[0] is not None:
                    index_map[row[0]] = {'table1': i}
            for i, row in enumerate(rows2):
                if len(row) > 0 and row[0] is not None:
                    if row[0] in index_map:
                        index_map[row[0]]['table2'] = i
                    else:
                        index_map[row[0]] = {'table2': i}
            
            for key, indices in index_map.items():
                row1 = rows1[indices.get('table1', -1)] if 'table1' in indices else [None] * len(headers1)
                row2 = rows2[indices.get('table2', -1)] if 'table2' in indices else [None] * len(headers2)
                merged_row = [None] * len(all_headers)
                
                for header in headers1:
                    merged_row[header_index_map[header]] = row1[headers1.index(header)]
                for header in headers2:
                    idx = header_index_map[header]
                    if merged_row[idx] is not None and row2[headers2.index(header)] is not None:
                        if merged_row[idx] != row2[headers2.index(header)]:
                            if conflict_resolution == 'table1':
                                pass  
                            elif conflict_resolution == 'table2':
                                merged_row[idx] = row2[headers2.index(header)]
                            elif conflict_resolution == 'raise':
                                raise ValueError(f"Conflict at index '{key}', column '{header}'")
                    else:
                        merged_row[idx] = row2[headers2.index(header)]
                
                merged_table.append(merged_row)
        
        return merged_table

if __name__ == "__main__":
    pass
