#pragma once

#include <iomanip>
#include <string>
#include <sstream>
#include <vector>

struct QueryRequest {
    std::string id;
    std::string line;
};

// Helper function to parse IN list from tuple syntax: ('val1', 'val2', ...)
std::vector<std::string> parse_in_list(std::istringstream& iss) {
    std::vector<std::string> result;

    // Read opening parenthesis
    char c;
    iss >> std::ws >> c;
    if (c != '(') {
        std::ostringstream oss;
        oss << "Expected '(' at start of IN list, but got '"
            << c << "' (int=" << static_cast<int>(static_cast<unsigned char>(c)) << ")";
        throw std::runtime_error(oss.str());
    }

    bool first = true;
    while (iss >> std::ws) {
        // Check for closing parenthesis
        if (iss.peek() == ')') {
            iss.get(); // consume ')'
            break;
        }

        // Skip comma after first element
        if (!first) {
            iss >> std::ws >> c;
            if (c != ',') {
                throw std::runtime_error("Expected ',' between IN list elements");
            }
        }
        first = false;

        std::string value;
        iss >> std::ws;
        if (iss.peek() == '\'') {
            iss.get();
            while (iss) {
                const char ch = static_cast<char>(iss.get());
                if (!iss) break;
                if (ch == '\'') {
                    if (iss.peek() == '\'') {
                        iss.get();
                        value.push_back('\'');
                        continue;
                    }
                    break;
                }
                value.push_back(ch);
            }
        } else {
            while (iss && iss.peek() != ',' && iss.peek() != ')') {
                value.push_back(static_cast<char>(iss.get()));
            }
            const auto start = value.find_first_not_of(" \t\r\n");
            const auto end = value.find_last_not_of(" \t\r\n");
            if (start == std::string::npos) {
                value.clear();
            } else {
                value = value.substr(start, end - start + 1);
            }
        }

        result.push_back(value);
    }

    return result;
}


//Q1a
struct Q1aArgs {
    std::vector<std::string> ID1;
    std::vector<std::string> ID2;
    std::vector<std::string> INFO1;
    std::vector<std::string> INFO2;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
    std::vector<std::string> GENDER;
    std::string YEAR1;
    std::string YEAR2;
};
inline Q1aArgs parse_q1a(const QueryRequest& request) {
    Q1aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q1a: failed to parse query id"); 
    }

	args.ID1 = parse_in_list(iss);
	args.ID2 = parse_in_list(iss);
	args.INFO1 = parse_in_list(iss);
	args.INFO2 = parse_in_list(iss);
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.GENDER = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q1a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q1a: failed to parse YEAR2");
	}

    return args;
}
    
//Q2a
struct Q2aArgs {
    std::vector<std::string> ID1;
    std::vector<std::string> ID2;
    std::vector<std::string> INFO1;
    std::vector<std::string> INFO2;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
    std::vector<std::string> GENDER;
    std::string YEAR1;
    std::string YEAR2;
};
inline Q2aArgs parse_q2a(const QueryRequest& request) {
    Q2aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q2a: failed to parse query id"); 
    }

	args.ID1 = parse_in_list(iss);
	args.ID2 = parse_in_list(iss);
	args.INFO1 = parse_in_list(iss);
	args.INFO2 = parse_in_list(iss);
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.GENDER = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q2a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q2a: failed to parse YEAR2");
	}

    return args;
}
    
//Q2b
struct Q2bArgs {
    std::vector<std::string> ID1;
    std::vector<std::string> ID2;
    std::vector<std::string> INFO1;
    std::vector<std::string> INFO2;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
    std::vector<std::string> GENDER;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> KEYWORD;
};
inline Q2bArgs parse_q2b(const QueryRequest& request) {
    Q2bArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q2b: failed to parse query id"); 
    }

	args.ID1 = parse_in_list(iss);
	args.ID2 = parse_in_list(iss);
	args.INFO1 = parse_in_list(iss);
	args.INFO2 = parse_in_list(iss);
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.GENDER = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q2b: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q2b: failed to parse YEAR2");
	}
	args.KEYWORD = parse_in_list(iss);

    return args;
}
    
//Q2c
struct Q2cArgs {
    std::vector<std::string> ID1;
    std::vector<std::string> ID2;
    std::vector<std::string> INFO1;
    std::vector<std::string> INFO2;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
    std::vector<std::string> GENDER;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> TITLE;
};
inline Q2cArgs parse_q2c(const QueryRequest& request) {
    Q2cArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q2c: failed to parse query id"); 
    }

	args.ID1 = parse_in_list(iss);
	args.ID2 = parse_in_list(iss);
	args.INFO1 = parse_in_list(iss);
	args.INFO2 = parse_in_list(iss);
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.GENDER = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q2c: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q2c: failed to parse YEAR2");
	}
	args.TITLE = parse_in_list(iss);

    return args;
}
    
//Q3a
struct Q3aArgs {
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> KEYWORD;
    std::vector<std::string> COUNTRY;
    std::vector<std::string> KIND1;
    std::vector<std::string> KIND2;
    std::vector<std::string> ROLE;
    std::vector<std::string> GENDER;
};
inline Q3aArgs parse_q3a(const QueryRequest& request) {
    Q3aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q3a: failed to parse query id"); 
    }

	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q3a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q3a: failed to parse YEAR2");
	}
	args.KEYWORD = parse_in_list(iss);
	args.COUNTRY = parse_in_list(iss);
	args.KIND1 = parse_in_list(iss);
	args.KIND2 = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.GENDER = parse_in_list(iss);

    return args;
}
    
//Q3b
struct Q3bArgs {
    std::string TITLE;
    std::string NAME_PCODE_NF;
    std::string NAME;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
};
inline Q3bArgs parse_q3b(const QueryRequest& request) {
    Q3bArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q3b: failed to parse query id"); 
    }

	if (!(iss >> std::quoted(args.TITLE))) {
		throw std::runtime_error("Q3b: failed to parse TITLE");
	}
	if (!(iss >> std::quoted(args.NAME_PCODE_NF))) {
		throw std::runtime_error("Q3b: failed to parse NAME_PCODE_NF");
	}
	if (!(iss >> std::quoted(args.NAME))) {
		throw std::runtime_error("Q3b: failed to parse NAME");
	}
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);

    return args;
}
    
//Q4a
struct Q4aArgs {
    std::vector<std::string> GENDER;
    std::vector<std::string> NAME_PCODE_NF;
    std::vector<std::string> NOTE;
    std::vector<std::string> ROLE;
    std::vector<std::string> ID;
};
inline Q4aArgs parse_q4a(const QueryRequest& request) {
    Q4aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q4a: failed to parse query id"); 
    }

	args.GENDER = parse_in_list(iss);
	args.NAME_PCODE_NF = parse_in_list(iss);
	args.NOTE = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.ID = parse_in_list(iss);

    return args;
}
    
//Q5a
struct Q5aArgs {
    std::vector<std::string> KIND;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> INFO1;
    std::vector<std::string> ID1;
    std::string ID2;
    std::string ID3;
    std::string INFO2;
    std::string INFO3;
    std::string INFO4;
    std::string INFO5;
};
inline Q5aArgs parse_q5a(const QueryRequest& request) {
    Q5aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q5a: failed to parse query id"); 
    }

	args.KIND = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q5a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q5a: failed to parse YEAR2");
	}
	args.INFO1 = parse_in_list(iss);
	args.ID1 = parse_in_list(iss);
	if (!(iss >> std::quoted(args.ID2))) {
		throw std::runtime_error("Q5a: failed to parse ID2");
	}
	if (!(iss >> std::quoted(args.ID3))) {
		throw std::runtime_error("Q5a: failed to parse ID3");
	}
	if (!(iss >> std::quoted(args.INFO2))) {
		throw std::runtime_error("Q5a: failed to parse INFO2");
	}
	if (!(iss >> std::quoted(args.INFO3))) {
		throw std::runtime_error("Q5a: failed to parse INFO3");
	}
	if (!(iss >> std::quoted(args.INFO4))) {
		throw std::runtime_error("Q5a: failed to parse INFO4");
	}
	if (!(iss >> std::quoted(args.INFO5))) {
		throw std::runtime_error("Q5a: failed to parse INFO5");
	}

    return args;
}
    
//Q6a
struct Q6aArgs {
    std::vector<std::string> KIND;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> INFO1;
    std::vector<std::string> ID1;
    std::string ID2;
    std::string ID3;
    std::string INFO2;
    std::string INFO3;
    std::string INFO4;
    std::string INFO5;
    std::vector<std::string> GENDER;
    std::vector<std::string> NAME_PCODE_NF;
    std::vector<std::string> NOTE;
    std::vector<std::string> ROLE;
    std::vector<std::string> ID4;
};
inline Q6aArgs parse_q6a(const QueryRequest& request) {
    Q6aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q6a: failed to parse query id"); 
    }

	args.KIND = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q6a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q6a: failed to parse YEAR2");
	}
	args.INFO1 = parse_in_list(iss);
	args.ID1 = parse_in_list(iss);
	if (!(iss >> std::quoted(args.ID2))) {
		throw std::runtime_error("Q6a: failed to parse ID2");
	}
	if (!(iss >> std::quoted(args.ID3))) {
		throw std::runtime_error("Q6a: failed to parse ID3");
	}
	if (!(iss >> std::quoted(args.INFO2))) {
		throw std::runtime_error("Q6a: failed to parse INFO2");
	}
	if (!(iss >> std::quoted(args.INFO3))) {
		throw std::runtime_error("Q6a: failed to parse INFO3");
	}
	if (!(iss >> std::quoted(args.INFO4))) {
		throw std::runtime_error("Q6a: failed to parse INFO4");
	}
	if (!(iss >> std::quoted(args.INFO5))) {
		throw std::runtime_error("Q6a: failed to parse INFO5");
	}
	args.GENDER = parse_in_list(iss);
	args.NAME_PCODE_NF = parse_in_list(iss);
	args.NOTE = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.ID4 = parse_in_list(iss);

    return args;
}
    
//Q7a
struct Q7aArgs {
    std::vector<std::string> KIND;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> INFO1;
    std::vector<std::string> ID1;
    std::string ID2;
    std::string ID3;
    std::string INFO2;
    std::string INFO3;
    std::string INFO4;
    std::string INFO5;
    std::vector<std::string> GENDER;
    std::vector<std::string> NAME_PCODE_NF;
    std::vector<std::string> NOTE;
    std::vector<std::string> ROLE;
    std::vector<std::string> ID4;
};
inline Q7aArgs parse_q7a(const QueryRequest& request) {
    Q7aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q7a: failed to parse query id"); 
    }

	args.KIND = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q7a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q7a: failed to parse YEAR2");
	}
	args.INFO1 = parse_in_list(iss);
	args.ID1 = parse_in_list(iss);
	if (!(iss >> std::quoted(args.ID2))) {
		throw std::runtime_error("Q7a: failed to parse ID2");
	}
	if (!(iss >> std::quoted(args.ID3))) {
		throw std::runtime_error("Q7a: failed to parse ID3");
	}
	if (!(iss >> std::quoted(args.INFO2))) {
		throw std::runtime_error("Q7a: failed to parse INFO2");
	}
	if (!(iss >> std::quoted(args.INFO3))) {
		throw std::runtime_error("Q7a: failed to parse INFO3");
	}
	if (!(iss >> std::quoted(args.INFO4))) {
		throw std::runtime_error("Q7a: failed to parse INFO4");
	}
	if (!(iss >> std::quoted(args.INFO5))) {
		throw std::runtime_error("Q7a: failed to parse INFO5");
	}
	args.GENDER = parse_in_list(iss);
	args.NAME_PCODE_NF = parse_in_list(iss);
	args.NOTE = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.ID4 = parse_in_list(iss);

    return args;
}
    
//Q8a
struct Q8aArgs {
    std::vector<std::string> ID;
    std::vector<std::string> INFO;
    std::vector<std::string> KIND1;
    std::vector<std::string> ROLE;
    std::vector<std::string> GENDER;
    std::vector<std::string> NAME_PCODE_CF;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> NAME;
    std::vector<std::string> KIND2;
};
inline Q8aArgs parse_q8a(const QueryRequest& request) {
    Q8aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q8a: failed to parse query id"); 
    }

	args.ID = parse_in_list(iss);
	args.INFO = parse_in_list(iss);
	args.KIND1 = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	args.GENDER = parse_in_list(iss);
	args.NAME_PCODE_CF = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q8a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q8a: failed to parse YEAR2");
	}
	args.NAME = parse_in_list(iss);
	args.KIND2 = parse_in_list(iss);

    return args;
}
    
//Q9a
struct Q9aArgs {
    std::vector<std::string> ID1;
    std::vector<std::string> ID2;
    std::string INFO1;
    std::string INFO2;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
};
inline Q9aArgs parse_q9a(const QueryRequest& request) {
    Q9aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q9a: failed to parse query id"); 
    }

	args.ID1 = parse_in_list(iss);
	args.ID2 = parse_in_list(iss);
	if (!(iss >> std::quoted(args.INFO1))) {
		throw std::runtime_error("Q9a: failed to parse INFO1");
	}
	if (!(iss >> std::quoted(args.INFO2))) {
		throw std::runtime_error("Q9a: failed to parse INFO2");
	}
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);

    return args;
}
    
//Q9b
struct Q9bArgs {
    std::vector<std::string> ID1;
    std::vector<std::string> ID2;
    std::vector<std::string> INFO;
    std::string NAME;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
    std::string YEAR1;
    std::string YEAR2;
};
inline Q9bArgs parse_q9b(const QueryRequest& request) {
    Q9bArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q9b: failed to parse query id"); 
    }

	args.ID1 = parse_in_list(iss);
	args.ID2 = parse_in_list(iss);
	args.INFO = parse_in_list(iss);
	if (!(iss >> std::quoted(args.NAME))) {
		throw std::runtime_error("Q9b: failed to parse NAME");
	}
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q9b: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q9b: failed to parse YEAR2");
	}

    return args;
}
    
//Q10a
struct Q10aArgs {
    std::vector<std::string> ID;
    std::vector<std::string> INFO;
    std::string NAME;
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
};
inline Q10aArgs parse_q10a(const QueryRequest& request) {
    Q10aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q10a: failed to parse query id"); 
    }

	args.ID = parse_in_list(iss);
	args.INFO = parse_in_list(iss);
	if (!(iss >> std::quoted(args.NAME))) {
		throw std::runtime_error("Q10a: failed to parse NAME");
	}
	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);

    return args;
}
    
//Q11a
struct Q11aArgs {
    std::string KIND;
    std::vector<std::string> ROLE;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> ID;
    std::string INFO;
    std::string NAME;
};
inline Q11aArgs parse_q11a(const QueryRequest& request) {
    Q11aArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q11a: failed to parse query id"); 
    }

	if (!(iss >> std::quoted(args.KIND))) {
		throw std::runtime_error("Q11a: failed to parse KIND");
	}
	args.ROLE = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q11a: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q11a: failed to parse YEAR2");
	}
	args.ID = parse_in_list(iss);
	if (!(iss >> std::quoted(args.INFO))) {
		throw std::runtime_error("Q11a: failed to parse INFO");
	}
	if (!(iss >> std::quoted(args.NAME))) {
		throw std::runtime_error("Q11a: failed to parse NAME");
	}

    return args;
}
    
//Q11b
struct Q11bArgs {
    std::vector<std::string> KIND;
    std::vector<std::string> ROLE;
    std::string YEAR1;
    std::string YEAR2;
    std::vector<std::string> ID1;
    std::string INFO1;
    std::string INFO2;
    std::vector<std::string> ID2;
};
inline Q11bArgs parse_q11b(const QueryRequest& request) {
    Q11bArgs args;
    std::istringstream iss(request.line);

    std::string qid;
    if (!(iss >> qid)) {  // consume query id
		throw std::runtime_error("Q11b: failed to parse query id"); 
    }

	args.KIND = parse_in_list(iss);
	args.ROLE = parse_in_list(iss);
	if (!(iss >> std::quoted(args.YEAR1))) {
		throw std::runtime_error("Q11b: failed to parse YEAR1");
	}
	if (!(iss >> std::quoted(args.YEAR2))) {
		throw std::runtime_error("Q11b: failed to parse YEAR2");
	}
	args.ID1 = parse_in_list(iss);
	if (!(iss >> std::quoted(args.INFO1))) {
		throw std::runtime_error("Q11b: failed to parse INFO1");
	}
	if (!(iss >> std::quoted(args.INFO2))) {
		throw std::runtime_error("Q11b: failed to parse INFO2");
	}
	args.ID2 = parse_in_list(iss);

    return args;
}
    