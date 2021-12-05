# Klimaton backend and data analisis

## Opis plików

plik "main.py" - główny plik backendu wykorzystujący fastAPI do utworzenia serwera i stworzenia ednpoint'ów do którch
żądania może wysyłać frontend. Plik wywołuje funkcje z plików *_data.py w celu wygenerowania odpowiednich danych/wykresów
i przekazuje jest frontendowi poprzez REST'owe API

pliki "*_data.py" - skrypty pythona wywoływane z pliku "main.py" zawierające funkcje wczytujące dane z plików Excela,
robiące preproccessing danych a potem zwracających dane w formie wymaganej przez stronę frontendową

plik "notebook_citiznes.ipynb" - notebook jupytera służący do testów "citizens_data.py"

pliki "eda1.ipynb" do "eda4.ipynb"  - notebooki Jupyterowe z EDA (exploratory data anaylisis) - czyli wstępną analizą danych.
Nie są one wykorzystane w aplikacji roboczej, ale zdecydowlaiśmy się zostawić je w repozytorium, ponieważ
w przypadku przy złych zmian w danych czy pojawieniu się dodatkowych tabel, kod zawarty w nich pomoże szybko przeanalizować
dane developerowi i odpowiednio użyć ich w aplikacji





