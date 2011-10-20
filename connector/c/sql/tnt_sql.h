#ifndef TNT_SQL_H_INCLUDED
#define TNT_SQL_H_INCLUDED

/*
 * Copyright (C) 2011 Mail.RU
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * @defgroup SQL
 * @ingroup  Operations
 * @brief Server Query Language support
 *
 * @{
 */

/**
 * Server query operation.
 *
 * Parses and processes user supplied SQL query .
 *
 * @param t handler pointer
 * @param q sql query string
 * @param qsize query size
 * @param e string error description
 * @returns number of operations processed on success, -1 on error and
 * string description returned (must be freed after use)
 */
int tnt_query(struct tnt *t, char *q, size_t qsize, char **e);

/**
 * Server query validation.
 *
 * Tells if the supplied query should be processed as SQL .
 *
 * @param q query string
 * @param qsize query size
 * @returns 0 if not, 1 if yes
 */
int tnt_query_is(char *q, size_t qsize);
/** @} */

#endif /* TNT_SQL_H_INCLUDED */
