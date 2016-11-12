import ExtendableError from 'es6-error';

class CustomError extends ExtendableError {
  constructor(message = '', data = { }) {
    super(message);
    this.data = data;
  }
}

export class NoOperationError extends CustomError { }

export default CustomError;
