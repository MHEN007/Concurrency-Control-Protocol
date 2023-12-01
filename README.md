# Concurrency-Control-Protocol
Tugas Besar Manajemen Basis Data IF3140

## Anggota Kelompok
| NIM  | NAMA |
| :---: | :---: |
| 13521007 | Matthew Mahendra |
| 13521018 | Syarifa Dwi Purnamasari |
| 13521028 | M. Zulfiansyah Bayu Pratama |
| 13521029 | M. Malik I. Baharsyah |

## Cara Penggunaan Program
Pastikan Java SDK sudah terpasang pada perangkat Anda.

Masukkan schedule dengan contoh sebagai berikut
```
R1(A);R2(B);C2;C1
```

Kompilasi masing-masing protokol pada folder `src/protocols` dengan perintah berikut
```
javac *
```

Untuk menjalankan protokol yang diingkan dapat mengikuti perintah berikut pada folder yang sama
1. 2PL: `java TwoPhaseLocking`
2. OCC: `java OptimisticConcurrencyControl`
3. MVCC: `java MVCC`