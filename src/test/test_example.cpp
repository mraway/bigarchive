#include <exception.h>
#include <iostream>

using namespace std;

void func() {
 throw StreamCorruptedException("stream corrrrrrrrrrrrrrrupted !! ");
}

int main() {
 try {
  func();
 }catch(ExceptionBase& e) {
   cout << "stream corruped exception throw-ed "; // << e.ToString();
 }
 cout << "end of main";
}


