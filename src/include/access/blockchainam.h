/*-------------------------------------------------------------------------
 *
 * blockchainam.h
 *	  Header for the blockchain table access method.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/blockchainam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOCKCHAINAM_H
#define BLOCKCHAINAM_H

#include "access/tableam.h"

/*
 * GetBlockchainAmTableAmRoutine
 *		Returns the TableAmRoutine struct for the blockchain access method.
 */
extern const TableAmRoutine *GetBlockchainAmTableAmRoutine(void);

#endif							/* BLOCKCHAINAM_H */
