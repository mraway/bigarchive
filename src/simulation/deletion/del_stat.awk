#!/bin/awk -f
BEGIN {newidx = 0; delidx = 0;}

{
    if ($2 ~ "add") {
        new[newidx] = $3;
        newidx ++;
    }
    if ($2 ~ "del") {
        del[delidx] = $3;
        delidx ++;
    }
}

END {
    totnew[0] = new[0];
    for (i = 1; i < newidx; i++) {
        totnew[i] = new[i] + totnew[i-1];
    }
 
    afterdel[newidx - 1] = totnew[newidx - 1];
    for (i = (newidx - 2); i >= 0; i--) {
        afterdel[i] = afterdel[i+1] - del[i+1];
    }

    for (i = 0; i < newidx; i++) {
        print (afterdel[i] - totnew[i])/afterdel[i];
    }
}