#include <exception.h>
#include <iostream>

using namespace std;

void func() {
 throw StreamCorruptedException();
}

int main() {
 try {
  func();
 }catch(ExceptionBase& e) {
   cout << e.ToString();
 }
 cout << "end of main";
}


