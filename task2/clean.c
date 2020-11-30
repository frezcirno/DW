#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <ctype.h>
#include <time.h>

#define CHUNK_SIZE 1024

int main()
{
    FILE *fp = fopen("movies_text.txt", "rb");
    FILE *fp1 = fopen("movies_text_out.txt", "w");

    long long t = time(NULL);
    long long sum = 0;

    char buf[CHUNK_SIZE], *p;

    while (!feof(fp)) {
        sum += fread(buf, 1, CHUNK_SIZE, fp);
        p = buf;
        for (int i = 0; i < CHUNK_SIZE; i++) {
            if (ispunct(buf[i]) && buf[i] != '\'' && buf[i] != '&' || !isprint(buf[i]) && buf[i] != '\n') {
                if (*(p - 1) != ' ') *p++ = ' ';
            } else {
                *p++ = buf[i];
            }
        }
        fwrite(buf, 1, p - buf, fp1);

        if (sum % (1024 * 1024 * 50) == 0) { printf("%lld bytes\n", sum); }
    }

    printf("%lld bytes in %llds\n", sum, time(NULL) - t);

    fclose(fp);
    fclose(fp1);
    return 0;
}

// with open("movies.txt", encoding="iso-8859-1") as f,
//     open("movies_out.txt", "w", encoding="utf-8") as fo:
//     while True:
//         ch = f.read(1)
//         if not ch: break
//         if ch.isalpha():
//             fo.write(ch)
