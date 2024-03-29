#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>

#include "Comparison.h"


Comparison::Comparison()
{
}


Comparison::Comparison(const Comparison &copy_me)
{
	operand1 = copy_me.operand1;
	whichAtt1 = copy_me.whichAtt1;
	operand2 = copy_me.operand2;
	whichAtt2 = copy_me.whichAtt2;

	attType = copy_me.attType;

	op = copy_me.op;
}


void Comparison :: Print () {

	cout << "Att " << whichAtt1 << " from ";

	if (operand1 == Left)
		cout << "left record ";
	else if (operand1 == Right)
		cout << "right record ";
	else 
		cout << "literal record ";


	if (op == LessThan)
		cout << "< ";
	else if (op == GreaterThan)
		cout << "> ";
	else
		cout << "= ";


	cout << "Att " << whichAtt2 << " from ";

	if (operand2 == Left)
		cout << "left record ";
	else if (operand2 == Right)
		cout << "right record ";
	else
		cout << "literal record ";
 

	if (attType == Int)
		cout << "(Int)";
	else if (attType == Double)
		cout << "(Double)";
	else
		cout << "(String)";
}




OrderMaker :: OrderMaker() {
	numAtts = 0;
}

OrderMaker :: OrderMaker(Schema *schema) {
	numAtts = 0;

	int n = schema->GetNumAtts();
	Attribute *atts = schema->GetAtts();

	for (int i = 0; i < n; i++) {
		if (atts[i].myType == Int) {
			whichAtts[numAtts] = i;
			whichTypes[numAtts] = Int;
			numAtts++;
		}
	}
	for (int i = 0; i < n; i++) {
                if (atts[i].myType == Double) {
                        whichAtts[numAtts] = i;
                        whichTypes[numAtts] = Double;
                        numAtts++;
                }
        }
        for (int i = 0; i < n; i++) {
                if (atts[i].myType == String) {
                        whichAtts[numAtts] = i;
                        whichTypes[numAtts] = String;
                        numAtts++;
                }
        }
}


void OrderMaker :: Print () {
	printf("NumAtts = %5d\n", numAtts);
	for (int i = 0; i < numAtts; i++)
	{
		printf("%3d: %5d ", i, whichAtts[i]);
		if (whichTypes[i] == Int)
			printf("Int\n");
		else if (whichTypes[i] == Double)
			printf("Double\n");
		else
			printf("String\n");
	}
}


int CNF :: GetSortOrders (OrderMaker &left, OrderMaker &right) {

	left.numAtts = 0;
	right.numAtts = 0;

	for (int i = 0; i < numAnds; i++) {
		if (orLens[i] != 1) {
			continue;
		}
		if (orList[i][0].op != Equals) {
			continue;
		}
		if (!((orList[i][0].operand1 == Left && orList[i][0].operand2 == Right) ||
		      (orList[i][0].operand2 == Left && orList[i][0].operand1 == Right))) {
			//continue;		
		}
		if (orList[i][0].operand1 == Left) {
			left.whichAtts[left.numAtts] = orList[i][0].whichAtt1;
			left.whichTypes[left.numAtts] = orList[i][0].attType;	
		}	

		if (orList[i][0].operand1 == Right) {
                        right.whichAtts[right.numAtts] = orList[i][0].whichAtt1;
                        right.whichTypes[right.numAtts] = orList[i][0].attType;
                }

		if (orList[i][0].operand2 == Left) {
                        left.whichAtts[left.numAtts] = orList[i][0].whichAtt2;
                        left.whichTypes[left.numAtts] = orList[i][0].attType;
                }

		if (orList[i][0].operand2 == Right) {
                        right.whichAtts[right.numAtts] = orList[i][0].whichAtt2;
                        right.whichTypes[right.numAtts] = orList[i][0].attType;
                }
		left.numAtts++;
		right.numAtts++;
	}
	
	return left.numAtts;
	
}


void CNF :: Print () {

	for (int i = 0; i < numAnds; i++) {
		
		cout << "( ";
		for (int j = 0; j < orLens[i]; j++) {
			orList[i][j].Print ();
			if (j < orLens[i] - 1)
				cout << " OR ";
		}
		cout << ") ";
		if (i < numAnds - 1)
			cout << " AND\n";
		else
			cout << "\n";
	}
}

void AddLitToFile (int &numFieldsInLiteral, FILE *outRecFile, FILE *outSchemaFile, char *value, Type myType) {
        fprintf (outRecFile, "%s|", value);
        if (myType == Int) {
                fprintf (outSchemaFile, "att%d Int\n", numFieldsInLiteral);
        } else if (myType == Double) {
                fprintf (outSchemaFile, "att%d Double\n", numFieldsInLiteral);
        } else if (myType == String) {
                fprintf (outSchemaFile, "att%d String\n", numFieldsInLiteral);
        } else {
                cerr << "I don't know that type!!\n";
                exit (1);
        }
        numFieldsInLiteral++;
}



void CNF :: GrowFromParseTree (struct AndList *parseTree, Schema *leftSchema, 
	Schema *rightSchema, Record &literal) {

	CNF &cnf = *this;
	FILE *outRecFile = fopen ("sdafdsfFFDSDA", "w");
	FILE *outSchemaFile = fopen ("hkljdfgkSDFSDF", "w");
	fprintf (outSchemaFile, "BEGIN\ntempSchema\nwherever\n");
	int numFieldsInLiteral = 0;

	for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
		
		if (parseTree == NULL) {
			cnf.numAnds = whichAnd;
			break;
		}	
		struct OrList *myOr = parseTree->left;
		for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
			if (myOr == NULL) {
				cnf.orLens[whichAnd] = whichOr;
				break;
			}
			Type typeLeft;
			Type typeRight;
			if (myOr->left->left->code == NAME) {
				if (leftSchema->Find (myOr->left->left->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand1 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt1 =
						leftSchema->Find (myOr->left->left->value);	
					typeLeft = leftSchema->FindType (myOr->left->left->value);
				} else if (rightSchema->Find (myOr->left->left->value) != -1) {
                                        cnf.orList[whichAnd][whichOr].operand1 = Right;
                                        cnf.orList[whichAnd][whichOr].whichAtt1 =
                                                rightSchema->Find (myOr->left->left->value);
                                        typeLeft = rightSchema->FindType (myOr->left->left->value);
                                } else {
					cout << "ERROR: Could not find attribute " <<
						myOr->left->left->value << "\n";
					exit (1);	
				}
			} else if (myOr->left->left->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, String);
				typeLeft = String;

			} else if (myOr->left->left->code == INT) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Int);
				typeLeft = Int;
			} else if (myOr->left->left->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Double);
				typeLeft = Double;
	
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}
			if (myOr->left->right->code == NAME) {
				
				if (leftSchema->Find (myOr->left->right->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand2 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt2 =
						leftSchema->Find (myOr->left->right->value);	
					typeRight = leftSchema->FindType (myOr->left->right->value);
				} else if (rightSchema->Find (myOr->left->right->value) != -1) {
                                        cnf.orList[whichAnd][whichOr].operand2 = Right;
                                        cnf.orList[whichAnd][whichOr].whichAtt2 =
                                                rightSchema->Find (myOr->left->right->value);
					typeRight = rightSchema->FindType (myOr->left->right->value);
                                } else {
					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
					exit (1);	
				}
			} else if (myOr->left->right->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, String);
				typeRight = String;

			} else if (myOr->left->right->code == INT) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Int);
				typeRight = Int;

			} else if (myOr->left->right->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Double);
				typeRight = Double;
	
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}

			if (typeLeft != typeRight) {
				cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
					<< myOr->left->right->value << " were found to not match.\n";
				exit (1);
			}

			cnf.orList[whichAnd][whichOr].attType = typeLeft;
			if (myOr->left->code == LESS_THAN) {
				cnf.orList[whichAnd][whichOr].op = LessThan;	
			} else if (myOr->left->code == GREATER_THAN) {
				cnf.orList[whichAnd][whichOr].op = GreaterThan;	
			} else if (myOr->left->code == EQUALS) {
				cnf.orList[whichAnd][whichOr].op = Equals;
			} else {
				cerr << "BAD: found a comparison op I don't recognize.\n";
				exit (1);
			}
		
		}
	}

	fclose (outRecFile);
	fprintf (outSchemaFile, "END\n");
	fclose (outSchemaFile);

	outRecFile = fopen ("sdafdsfFFDSDA", "r");

	Schema mySchema("hkljdfgkSDFSDF", "tempSchema");

	literal.SuckNextRecord (&mySchema, outRecFile);


	fclose (outRecFile);

	remove("sdafdsfFFDSDA");
	remove("hkljdfgkSDFSDF");
}


void CNF :: GrowFromParseTree (struct AndList *parseTree, Schema *mySchema, 
	Record &literal) {

	CNF &cnf = *this;

	FILE *outRecFile = fopen ("sdafdsfFFDSDA", "w");

	// also as kind of a hack, the schema for the literal record is built up
	// inside of a text file, where it will also be read from subsequently
	FILE *outSchemaFile = fopen ("hkljdfgkSDFSDF", "w");
	fprintf (outSchemaFile, "BEGIN\ntempSchema\nwherever\n");

	// this tells us the size of the literal record
	int numFieldsInLiteral = 0;

	// now we go through and build the comparison structure
	for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
		
		// see if we have run off of the end of all of the ANDs
		if (parseTree == NULL) {
			cnf.numAnds = whichAnd;
			break;
		}	

		// we have not, so copy over all of the ORs hanging off of this AND
		struct OrList *myOr = parseTree->left;
		for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

			// see if we have run off of the end of the ORs
			if (myOr == NULL) {
				cnf.orLens[whichAnd] = whichOr;
				break;
			}

			// we have not run off the list, so add the current OR in!
			
			// these store the types of the two values that are found
			Type typeLeft;
			Type typeRight;

			// first thing is to deal with the left operand
			// so we check to see if it is an attribute name, and if so,
			// we look it up in the schema
			if (myOr->left->left->code == NAME) {
				
				// see if we can find this attribute in the schema
				if (mySchema->Find (myOr->left->left->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand1 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt1 =
						mySchema->Find (myOr->left->left->value);	
					typeLeft = mySchema->FindType (myOr->left->left->value);

				// it is not there!  So there is an error in the query
                                } else {
					cout << "ERROR: Could not find attribute " <<
						myOr->left->left->value << "\n";
					exit (1);	
				}

			// the next thing is to see if we have a string; if so, add it to the 
			// literal record that stores all of the comparison values
			} else if (myOr->left->left->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, String);
				typeLeft = String;

			// see if it is an integer
			} else if (myOr->left->left->code == INT) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Int);
				typeLeft = Int;

			// see if it is a double
			} else if (myOr->left->left->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Double);
				typeLeft = Double;
	
			// catch-all case
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}

			// now that we have dealt with the left operand, we need to deal with the
			// right operand
			if (myOr->left->right->code == NAME) {
				
				// see if we can find this attribute in the left schema
				if (mySchema->Find (myOr->left->right->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand2 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt2 =
						mySchema->Find (myOr->left->right->value);	
					typeRight = mySchema->FindType (myOr->left->right->value);

				// it is not there!  So there is an error in the query
                                } else {
					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
					exit (1);	
				}

			// the next thing is to see if we have a string; if so, add it to the 
			// literal record that stores all of the comparison values
			} else if (myOr->left->right->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, String);
				typeRight = String;

			// see if it is an integer
			} else if (myOr->left->right->code == INT) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Int);
				typeRight = Int;

			// see if it is a double
			} else if (myOr->left->right->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Double);
				typeRight = Double;
	
			// catch-all case
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}
			

			// now we check to make sure that there was not a type mismatch
			if (typeLeft != typeRight) {
				cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
					<< myOr->left->right->value << " were found to not match.\n";
				exit (1);
			}

			// set up the type info for this comparison
			cnf.orList[whichAnd][whichOr].attType = typeLeft;

			// and finally set up the comparison operator for this comparison
			if (myOr->left->code == LESS_THAN) {
				cnf.orList[whichAnd][whichOr].op = LessThan;	
			} else if (myOr->left->code == GREATER_THAN) {
				cnf.orList[whichAnd][whichOr].op = GreaterThan;	
			} else if (myOr->left->code == EQUALS) {
				cnf.orList[whichAnd][whichOr].op = Equals;
			} else {
				cerr << "BAD: found a comparison op I don't recognize.\n";
				exit (1);
			}
		
		}
	}

	// the very last thing is to set up the literal record; first close the
	// file where its information has been stored
	fclose (outRecFile);
	fprintf (outSchemaFile, "END\n");
	fclose (outSchemaFile);

	// and open up the record file
	outRecFile = fopen ("sdafdsfFFDSDA", "r");

	// read in the record's schema
	Schema outSchema("hkljdfgkSDFSDF", "tempSchema");

	// and get the record
	literal.SuckNextRecord (&outSchema, outRecFile);

	// close the record file
	fclose (outRecFile);

	remove("sdafdsfFFDSDA");
	remove("hkljdfgkSDFSDF");
}

