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
Inainte de aceasta operatie, am impartit fiecare fisier in mai multe fragmente de dimensiuni aproximativ egale folosind o dimensiune statica de caractere. Astfel se garanteaza o munca distribuita egala intre thread-uri (presupunand ca nu exista cuvinte exagerat de lungi). Aceste fragmente sunt pe urma introduse intr-o coada, de unde pot fi extrase de catre fiecare worker thread de tip Map, folosind un mutex pe coada. <br>
Dupa ce un thread si-a extras un fragment de text, caruia ii este asociat un ID de fisier, acesta va rula procesul de mapare, care consta in asocierea cuvintelor cu indexul fisierului memorat de fragment folosind un hashmap.