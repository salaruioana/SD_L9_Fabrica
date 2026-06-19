
Ghid de Pornire: Fabrica de Flux (DDD + Data Flow)
## Pasul 1: Pregătirea terenului (Fisierele TXT)
- Înainte de orice, mută fișierele de configurare în folderul Home al utilizatorului, deoarece codul le caută dinamic prin user.home (/home/student/):

cp "/home/student/SD/Lab9/SD_Laborator_09/exemplu 2/produse.txt" /home/student/

cp "/home/student/SD/Lab9/SD_Laborator_09/exemplu 2/clienti_baza_date.txt" /home/student/
## Pasul 2: Curățarea și Recompilarea codului
- Intră în folderul principal al proiectului (exemplu 2) și compilează toate cele 5 microservicii dintr-o singură comandă:

cd "/home/student/SD/Lab9/SD_Laborator_09/exemplu 2"

mvn clean package

## Pasul 3: Curățarea serverului Data Flow (În Shell)
- Deschide terminalul cu Data Flow Shell și curăță stream-ul și registrele vechi pentru a evita conflictele de căi:

stream destroy --name fabrica-flux
app unregister --name client --type source
app unregister --name comanda --type processor
app unregister --name depozit --type processor
app unregister --name facturare --type processor
app unregister --name livrare --type sink
## Pasul 4: Înregistrarea modulelor în Data Flow (Register)
- Rulează pe rând comenzile de înregistrare, folosind ghilimelele pentru a proteja spațiul din numele folderului exemplu 2:

app register --name client --type source --uri "file:///home/student/SD/Lab9/SD_Laborator_09/exemplu 2/Client/target/Client-1.0-SNAPSHOT.jar"
app register --name comanda --type processor --uri "file:///home/student/SD/Lab9/SD_Laborator_09/exemplu 2/ComandaMicroservice/target/ComandaMicroservice-1.0-SNAPSHOT.jar"
app register --name depozit --type processor --uri "file:///home/student/SD/Lab9/SD_Laborator_09/exemplu 2/DepozitMicroservice/target/DepozitMicroservice-1.0-SNAPSHOT.jar"
app register --name facturare --type processor --uri "file:///home/student/SD/Lab9/SD_Laborator_09/exemplu 2/FacturareMicroservice/target/FacturareMicroservice-1.0-SNAPSHOT.jar"
app register --name livrare --type sink --uri "file:///home/student/SD/Lab9/SD_Laborator_09/exemplu 2/Livrare/target/Livrare-1.0-SNAPSHOT.jar"
## Pasul 5: Crearea și Lansarea Pipeline-ului (Deploy)
- Definește lanțul de componente și pornește-l, injectând credențialele pentru containerul local de RabbitMQ:

stream create --name fabrica-flux --definition "client | comanda | depozit | facturare | livrare"
stream deploy --name fabrica-flux --properties "app.*.spring.rabbitmq.username=student,app.*.spring.rabbitmq.password=student"
## Pasul 6: Verificarea și Testul Suprem (Urmărirea logurilor)
- Deschide un terminal normal de Linux și agață-te de logul de ieșire al microserviciului Livrare pentru a vedea avizele cum curg live:

tail -f $(find /tmp -name "stdout*.log" | grep "livrare")
Ar trebui să vezi cum se printează în cascadă block-urile cu:
=================== AVIZ DE EXPEDIȚIE ===================