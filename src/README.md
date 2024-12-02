# Implementare generala
Am implementat paradigma Map-Reduce multithreaded in mod dinamic.
Pentru operatiile de Map respectiv Reduce am cate o coada care contine fragmente de sarcini.
La orice moment de timp, daca un thread este liber isi alege din coada una sau mai multe fragmente pentru indeplinirea operatiei sale. <br>
Fragmentele sunt de tip:
- Mapper - memoreaza fisierul sursa prin ID-ul sau. <br>
- Reducer - reprezinta un index inversat partial generat de un fragment de Mapper (fisier) <br>

In contrast cu thread-urile de Map, cele de Reduce necesita 2 fragmente de extras din coada. 
Astfel, desi thread-urile Reduce sunt active de la inceput, acestea trebuie sa astepte sa primeasca date de la thread-urile Map.
Mai jos este implementarea pe scurt a fiecarui thread corespondent fie operatiei de Map, fie de Reduce.

### Map
```cpp
while (true)
{
    // Verifica coada si extrage sarcina
    mutexMappers.lock()
    if (queueMappers.empty())
        mutexMappers.unlock()
        mutexIncrement.lock()
            mappersDone++
        mutexIncrement.unlock()
        break;
    fileChunk = queueMappers.pop()
    mutexMappers.unlock()

    reverseIndex = Map(fileChunk)

    // Paseaza munca pentru thread-urile de reducere
    mutexReducers.lock()
    queueReducers.push(reverseIndex)
    mutexReducers.unlock()
}
```
### Reduce
```cpp
while (true)
{
    mutexReducers.lock()
    mutexMappersDone.lock()
    if (queue.size() < 2)
    {
        mutexReducers.unlock()
        if (mappersDone == mappersCount)
            mutexMappersDone.unlock()
            break
        mutexMappersDone.unlock()
        continue
    }

    map1 = queueReducers.pop()
    map2 = queueReducers.pop()
    mutexReducers.unlock();

    mapResult = Combine(map1, map2)

    mutexReducers.lock()
    queueReducers.push(mapResult)
    mutexReducers.unlock()
}
```

# Map
Inainte de aceasta operatie, am impartit fiecare fisier in mai multe fragmente de dimensiuni aproximativ egale folosind o dimensiune statica de caractere. Astfel se garanteaza o munca distribuita egala intre thread-uri (presupunand ca nu exista cuvinte exagerat de lungi). Aceste fragmente sunt pe urma introduse intr-o coada, de unde pot fi extrase de catre fiecare worker thread de tip Map, folosind un mutex pe coada. Aceasta coada face parte din memoria partajata a thread-urilor Map, pe langa alte esentiale cum ar fi mutex-urile, coada de Reduce in care se adauga rezultatele si contorul pentru thread-uri terminate. <br>
Dupa ce un thread si-a extras un fragment de text din coada, caruia ii este asociat un ID de fisier, acesta va rula procesul de mapare, care consta in asocierea cuvintelor cu indexul fisierului memorat de fragment folosind un hashmap, acesta fiind reprezentarea unui Inverted Index in aceasta implementare. Odata terminat, respectivul hashmap este pasat mai departe pentru a fi procesat de catre thread-urile de tip Reduce.
# Reduce
Intrucat thread-urile Map produc hashmap-uri independente una de cealalta, thread-urile Reduce se pot apuca de procesat imediat cum primesc cel putin 2 harti
de procesat. La fel ca la Map, se foloseste o coada din care fiecare thread extrage 2 hashmap-uri. Este nevoie de un mutex pentru operatiile pe coada, insa dupa s-au extras hartile, acestea se pot combina in memoria locala a thread-ului. Rezultatul este pe urma introdus inapoi in coada.<br>
Thread-urile Reduce se opresc atunci cand mai ramane o harta in coada si toate thread-urile Map si-au terminat treaba, deci nu se mai produc harti de combinat.<br>
## Scriere in fisiere
Thread-urile Reduce sunt responsabile si de scrierea in fisiere, care se face astfel:<br>
- Thread-ul 0 de tip Reducer parcurge ultimul Inverted Index ramas si imparte cuvintele in galeti corespondente fisierului in care trebuie scrise
- Fiecare thread primeste o portiune din vectorul de galeti
- Thread-urile scriu in fisierele corespondente cuvintele din galetile pe care le-au primit, asigurand ordinea corecta a cuvintelor impreuna cu fisierele in care s-au regasit