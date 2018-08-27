import IPython
import numpy as np
from IPython.core.display import SVG
from keras import Model, Input
from keras.layers import Dropout, Dense, Bidirectional, TimeDistributed, LSTM, Embedding, concatenate, GlobalMaxPool1D, \
    MaxPooling1D, Conv1D
from keras.utils.vis_utils import model_to_dot
from nltk import sent_tokenize


class CharCNN:
    CHAR_SET = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .!?:,\'%-\(\)/$|&;[]"'

    def __init__(self, max_len_of_sentence, max_num_of_setnence, verbose=10):
        """

        :param max_len_of_sentence:
        :param max_num_of_setnence:
        :param verbose:
        """
        self.max_len_of_sentence = max_len_of_sentence
        self.max_num_of_setnence = max_num_of_setnence
        self.verbose = verbose

        self.num_of_char = 0
        self.num_of_label = 0
        self.unknown_label = ''

    def preprocess(self, labels, char_dict=None, unknown_label='UNK'):
        """

        :param labels:
        :param char_dict:
        :param unknown_label:
        :return:
        """
        if self.verbose > 3:
            print('-----> Stage: preprocess')
        if self.verbose > 5:
            print("5")
            print(labels)

        self.build_char_dictionary(char_dict, unknown_label)
        self.convert_labels(labels)

    def build_char_dictionary(self, char_set=None, unknown_label='UNK'):
        """

        :param char_set: Define possible char set.
        :param unknown_label: Using "UNK" if character does not exist in this set
        :return:
        """

        if char_set is None:
            char_set = self.CHAR_SET

        self.unknown_label = unknown_label
        # Remove duplicated and convert to list
        chars = list(set(char_set))

        # Insert this unknown_label if character does not exist
        chars.insert(0, unknown_label)

        self.num_of_char = len(chars)
        self.char_indices = dict((c, i) for i, c in enumerate(chars))
        self.indices_char = dict((i, c) for i, c in enumerate(chars))

        if self.verbose > 5:
            print('Totoal number of chars:', self.num_of_char)

            print('First 3 char_indices sample:', {k: self.char_indices[k] for k in list(self.char_indices)[:3]})
            print('First 3 indices_char sample:', {k: self.indices_char[k] for k in list(self.indices_char)[:3]})

        return self.char_indices, self.indices_char, self.num_of_char

    def convert_labels(self, labels):
        """
        Convert the category label to numeric
        :param labels:
        :return:
        """

        self.label2indexes = dict((l, i) for i, l in enumerate(labels))
        self.index2labels = dict((i, l) for i, l in enumerate(labels))

        if self.verbose > 5:
            print('Label to Index: ', self.label2indexes)
            print('Index to Label: ', self.index2labels)

        self.num_of_label = len(self.label2indexes)

        return self.label2indexes, self.index2labels

    def _transform_raw_data(self, df, x_col, y_col, label2indexes=None, sample_size=None):
        """
        Transform raw data to list
        :param df:
        :param x_col:
        :param y_col:
        :param label2indexes:
        :param sample_size:
        :return:
        """

        x = []
        y = []

        actual_max_sentence = 0

        if sample_size is None:
            sample_size = len(df)

        for i, row in df.head(sample_size).iterrows():
            x_data = row[x_col]
            y_data = row[y_col]

            sentences = sent_tokenize(x_data)
            x.append(sentences)

            if len(sentences) > actual_max_sentence:
                actual_max_sentence = len(sentences)

            y.append(label2indexes[y_data])

        if self.verbose > 5:
            print('Number of news: %d' % (len(x)))
            print('Actual max sentence: %d' % actual_max_sentence)
            print(label2indexes)

        return x, y

    def _transform_training_data(self, x_raw, y_raw, max_len_of_sentence=None, max_num_of_setnence=None):
        """
        Transform pre-processed data to numpy
        :param x_raw:
        :param y_raw:
        :param max_len_of_sentence:
        :param max_num_of_setnence:
        :return:
        """
        unknown_value = self.char_indices[self.unknown_label]

        x = np.ones((len(x_raw), max_num_of_setnence, max_len_of_sentence), dtype=np.int64) * unknown_value
        y = np.array(y_raw)

        if max_len_of_sentence is None:
            max_len_of_sentence = self.max_len_of_sentence
        if max_num_of_setnence is None:
            max_num_of_setnence = self.max_num_of_setnence

        for i, doc in enumerate(x_raw):
            for j, sentence in enumerate(doc):
                if j < max_num_of_setnence:
                    for t, char in enumerate(sentence[-max_len_of_sentence:]):
                        if char not in self.char_indices:
                            x[i, j, (max_len_of_sentence - 1 - t)] = self.char_indices['UNK']
                        else:
                            x[i, j, (max_len_of_sentence - 1 - t)] = self.char_indices[char]

        return x, y

    def _build_character_block(self, block, dropout=0.3, filters=None, kernel_size=None,
                               pool_size=None, padding='valid', activation='relu',
                               kernel_initializer='glorot_normal'):
        """

        :param block:
        :param dropout:
        :param filters:
        :param kernel_size:
        :param pool_size:
        :param padding:
        :param activation:
        :param kernel_initializer:
        :return:
        """
        if filters is None:
            filters = [64, 100]
        if kernel_size is None:
            kernel_size = [3, 3]
        if pool_size is None:
            pool_size = [2, 2]

        for i in range(len(filters)):
            block = Conv1D(
                filters=filters[i], kernel_size=kernel_size[i],
                padding=padding, activation=activation, kernel_initializer=kernel_initializer)(block)

        block = Dropout(dropout)(block)
        block = MaxPooling1D(pool_size=pool_size[i])(block)

        block = GlobalMaxPool1D()(block)
        block = Dense(128, activation='relu')(block)
        return block

    def _build_sentence_block(self, max_len_of_sentence, max_num_of_setnence,
                              char_dimension=16,
                              filters=None,
                              kernel_sizes=None,
                              pool_sizes=None,
                              dropout=0.4):

        if filters is None:
            filters = [[3, 5, 7], [200, 300, 300], [300, 400, 400]]

        if kernel_sizes is None:
            kernel_sizes = [[4, 3, 3], [5, 3, 3], [6, 3, 3]]

        if pool_sizes is None:
            pool_sizes = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]

        sent_input = Input(shape=(max_len_of_sentence,), dtype='int64')
        embedded = Embedding(self.num_of_char, char_dimension, input_length=max_len_of_sentence)(sent_input)

        blocks = []
        for i, filter_layers in enumerate(filters):
            blocks.append(
                self._build_character_block(
                    block=embedded, filters=filters[i], kernel_size=kernel_sizes[i], pool_size=pool_sizes[i])
            )

        sent_output = concatenate(blocks, axis=-1)
        sent_output = Dropout(dropout)(sent_output)
        sent_encoder = Model(inputs=sent_input, outputs=sent_output)

        return sent_encoder

    def _build_document_block(self, sent_encoder, max_len_of_sentence, max_num_of_setnence,
                              num_of_label, dropout=0.3,
                              loss='sparse_categorical_crossentropy', optimizer='rmsprop', metrics=None):
        """

        :param sent_encoder:
        :param max_len_of_sentence:
        :param max_num_of_setnence:
        :param num_of_label:
        :param dropout:
        :param loss:
        :param optimizer:
        :param metrics:
        :return:
        """
        if metrics is None:
            metrics = ['accuracy']
        doc_input = Input(shape=(max_num_of_setnence, max_len_of_sentence), dtype='int64')
        doc_output = TimeDistributed(sent_encoder)(doc_input)

        doc_output = Bidirectional(LSTM(128, return_sequences=False, dropout=dropout))(doc_output)

        doc_output = Dropout(dropout)(doc_output)
        doc_output = Dense(128, activation='relu')(doc_output)
        doc_output = Dropout(dropout)(doc_output)
        doc_output = Dense(num_of_label, activation='sigmoid')(doc_output)

        doc_encoder = Model(inputs=doc_input, outputs=doc_output)
        doc_encoder.compile(loss=loss, optimizer=optimizer, metrics=metrics)
        return doc_encoder

    def process(self, df, x_col, y_col,
                max_len_of_sentence=None,
                max_num_of_setnence=None, label2indexes=None, sample_size=None):
        """

        :param df:
        :param x_col:
        :param y_col:
        :param max_len_of_sentence:
        :param max_num_of_setnence:
        :param label2indexes:
        :param sample_size:
        :return:
        """
        if self.verbose > 3:
            print('-----> Stage: process')

        if label2indexes is None:
            if self.label2indexes is None:
                raise Exception('Does not initialize label2indexes. Please invoke pre-process step first')
            label2indexes = self.label2indexes
        if max_len_of_sentence is None:
            max_len_of_sentence = self.max_len_of_sentence
        if max_num_of_setnence is None:
            max_num_of_setnence = self.max_num_of_setnence

        x_preprocess, y_preprocess = self._transform_raw_data(
            df=df, x_col=x_col, y_col=y_col, label2indexes=label2indexes)


        x_preprocess, y_preprocess = self._transform_training_data(
            x_raw=x_preprocess, y_raw=y_preprocess,
            max_len_of_sentence=max_len_of_sentence, max_num_of_setnence=max_num_of_setnence)

        if self.verbose > 5:
            print('Shape: ', x_preprocess.shape, y_preprocess.shape)

        return x_preprocess, y_preprocess

    def build_model(self, char_dimension=16, display_summary=False, display_architecture=False,
                    loss='sparse_categorical_crossentropy', optimizer='rmsprop', metrics=None):
        """

        :param char_dimension:
        :param display_summary:
        :param display_architecture:
        :param loss:
        :param optimizer:
        :param metrics:
        :return:
        """
        if metrics is None:
            metrics = ['accuracy']

        if self.verbose > 3:
            print('-----> Stage: build model')

        sent_encoder = self._build_sentence_block(
            char_dimension=char_dimension,
            max_len_of_sentence=self.max_len_of_sentence, max_num_of_setnence=self.max_num_of_setnence)

        doc_encoder = self._build_document_block(
            sent_encoder=sent_encoder, num_of_label=self.num_of_label,
            max_len_of_sentence=self.max_len_of_sentence, max_num_of_setnence=self.max_num_of_setnence,
            loss=loss, optimizer=optimizer, metrics=metrics)

        if display_architecture:
            print('Sentence Architecture')
            IPython.display.display(SVG(model_to_dot(sent_encoder).create(prog='dot', format='svg')))
            print()
            print('Document Architecture')
            IPython.display.display(SVG(model_to_dot(doc_encoder).create(prog='dot', format='svg')))

        if display_summary:
            print(doc_encoder.summary())

        self.model = {
            'sent_encoder': sent_encoder,
            'doc_encoder': doc_encoder
        }

        return doc_encoder

    def train(self, x_train, y_train, x_test, y_test, batch_size=128, epochs=1, shuffle=True):
        """

        :param x_train:
        :param y_train:
        :param x_test:
        :param y_test:
        :param batch_size:
        :param epochs:
        :param shuffle:
        :return:
        """
        if self.verbose > 3:
            print('-----> Stage: train model')

        self.get_model().fit(
            x_train, y_train, validation_data=(x_test, y_test),
            batch_size=batch_size, epochs=epochs, shuffle=shuffle)

    #         return self.model['doc_encoder']

    def predict(self, x, return_prob=False):
        """

        :param x:
        :param return_prob:
        :return:
        """
        if self.verbose > 3:
            print('-----> Stage: predict')

        if return_prob:
            return self.get_model().predict(x_test)

        return self.get_model().predict(x_test).argmax(axis=-1)

    def get_model(self):
        return self.model['doc_encoder']
