# Implementare generala
Am implementat paradigma Map-Reduce multithreaded in mod dinamic.
Pentru operatiile de Map respectiv Reduce am cate o coada care contine fragmente de sarcini.
La orice moment de timp, daca un thread este liber isi alege din coada una sau mai multe fragmente pentru indeplinirea operatiei sale. 
Fiecare fragment memoreaza fisierul sursa prin ID-ul sau. <br>
Thread-urile pentru Reduce incep dupa ce au terminat toate mappere-le. Asta se faciliteaza cu o bariera, care este prezenta in ambele functii de 

## Map
```cpp
while (true)
{
    // Este necesar un mutex intrucat coada este accesibila 
    // de catre toate thread-urile
    mutex.lock()
        if (queue.IsEmpty())
            return;
        fileChunk = queue.pop();
    mutex.unlock();

    reverseIndex = Map(fileChunk)
    threadReverseIndex.add(reverseIndex)
}
```
## Reduce
```cpp
while (true)
{
    // Este necesar un mutex intrucat coada este accesibila 
    // de catre toate thread-urile
    mutex.lock()
        if (queue.IsEmpty())
            return;
        fileChunk = queue.pop();
    mutex.unlock();

    reverseIndex = Map(fileChunk)
    threadReverseIndex.add(reverseIndex)
}
```