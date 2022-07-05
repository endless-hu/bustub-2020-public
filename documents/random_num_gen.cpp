#include <iostream>
#include <iomanip>
#include <string>
#include <map>
#include <random>
#include <cmath>
#include <fstream>
using namespace std;
int main()
{
  string out_filename;
  cout << "Please input output filename" << endl;
  cin >> out_filename;

  std::ofstream outf{out_filename};

  // If we couldn't open the output file stream for writing
  if (!outf)
  {
    // Print an error and exit
    std::cerr << "Uh oh, Sample.dat could not be opened for writing!" << std::endl;
    return 1;
  }

  // Seed with a real random value, if available
  std::random_device r;

  cout << "Please specify the range of random number: " << endl;
  cout << "between ";
  int low_bound;
  cin >> low_bound;
  int ceil;
  cout << "and  ";
  cin >> ceil;
  cout << "How many numbers do you wish to generate?" << endl;
  int num;
  cin >> num;

  // Choose a random mean
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(low_bound, ceil);
  for (int i = 0; i < num; i++)
  {
    int mean = uniform_dist(e1);
    std::cout << "Randomly-chosen mean: " << mean << '\n';
    outf << mean << endl;
  }
}
